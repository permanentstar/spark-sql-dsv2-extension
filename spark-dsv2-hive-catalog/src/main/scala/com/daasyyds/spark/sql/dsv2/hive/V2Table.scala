package com.daasyyds.spark.sql.dsv2.hive

import java.{util => jUtil}

import com.daasyyds.spark.sql.dsv2.hive.internal.CatalogUtil
import com.daasyyds.spark.sql.dsv2.hive.provider.write.{CsvProviderFileWriteBuilder, JsonProviderFileWriteBuilder}
import com.daasyyds.spark.sql.dsv2.hive.read.HiveFileFormatReadBuilder
import com.daasyyds.spark.sql.dsv2.hive.write.HiveFileFormatWriteBuilder
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.InternalSqlBridge.V1Table
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{Cast, GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.connector.catalog.{SupportsAtomicPartitionManagement, SupportsRead, SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.{FileStatusCache, InMemoryFileIndex}
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScanBuilder
import org.apache.spark.sql.execution.datasources.v2.json.JsonScanBuilder
import org.apache.spark.sql.hive.InternalHiveBridge.hiveClientImpl
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

private[hive] class V2Table(val catalogName: String, val v2Catalog: V2ExternalCatalog, val delegate: CatalogTable) extends V1Table(delegate) with SupportsRead with SupportsWrite with SupportsAtomicPartitionManagement with Logging {
  if (delegate.tableType == CatalogTableType.VIEW) {
    throw new UnsupportedOperationException(s"v2 datasource can not support view currently..`")
  }
  val ss: SparkSession = SparkSession.active
  val catalog: SessionCatalog = v2Catalog.sessionCatalog()
  val partitionSchema: StructType = delegate.partitionSchema
  val dataSchema: StructType = StructType(delegate.schema.take(delegate.schema.fields.length - partitionSchema.fields.length))
  val identifier: TableIdentifier = delegate.identifier
  val storage: CatalogStorageFormat = delegate.storage
  val stats: Option[CatalogStatistics] = delegate.stats
  val tracksPartitionsInCatalog: Boolean = delegate.tracksPartitionsInCatalog
  val provider: Option[String] = delegate.provider

  override def capabilities(): jUtil.Set[TableCapability] = {
    Set(BATCH_READ, BATCH_WRITE, TRUNCATE, OVERWRITE_BY_FILTER, OVERWRITE_DYNAMIC).asJava
  }

  override def toString(): String = s"V2Table($catalogName.$name)"

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val userSpecifiedSchema = if (partitionSchema.nonEmpty) {
      Some(partitionSchema)
    } else None
    val fileIndex = if (ss.sessionState.conf.manageFilesourcePartitions && tracksPartitionsInCatalog && partitionSchema.nonEmpty) {
      new ExternalCatalogFileIndex(ss, catalog, delegate, stats.map(_.sizeInBytes.toLong).getOrElse(ss.sessionState.conf.defaultSizeInBytes),
        options.asCaseSensitiveMap().asScala.toMap, userSpecifiedSchema, FileStatusCache.getOrCreate(ss))
    } else {
      new InMemoryFileIndex(ss, Seq(new Path(storage.locationUri.get.toString)), options.asCaseSensitiveMap().asScala.toMap,
        userSpecifiedSchema, FileStatusCache.getOrCreate(ss))
    }
    provider match {
      case Some(f) if f == "json" => new JsonScanBuilder(ss, fileIndex, delegate.schema, dataSchema, options)
      case Some(f) if f == "csv" => CSVScanBuilder(ss, fileIndex, delegate.schema, dataSchema, options)
      case Some(f) if f == "hive" => HiveFileFormatReadBuilder(ss, fileIndex, delegate.schema, dataSchema, this)
      case _ => throw new IllegalArgumentException(s"Datasource V2 Scan not support provider ${provider.getOrElse("NULL")} currently..")
    }
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val path = Seq(storage.locationUri.get.toString)
    provider match {
      case Some(f) if f == "json" => new JsonProviderFileWriteBuilder(path, "JSON", JsonProviderFileWriteBuilder.supportsDataType, info, this)
      case Some(f) if f == "csv" => new CsvProviderFileWriteBuilder(path, "CSV", CsvProviderFileWriteBuilder.supportsDataType, info, this)
      case Some(f) if f == "hive" => new HiveFileFormatWriteBuilder(path, hiveClientImpl.toHiveTable(delegate), info, this)
      case _ => throw new IllegalArgumentException(s"Datasource V2 Write not support provider ${provider.getOrElse("NULL")} currently..")
    }
  }

  override def createPartitions(idents: Array[InternalRow], properties: Array[jUtil.Map[String, String]]): Unit = {
    val parts = idents.map(ident => delegate.partitionColumnNames.zip(ident.asInstanceOf[GenericInternalRow].values).map(tp => (tp._1, tp._2.toString)).toMap)
      .zip(properties).map(tp => CatalogTablePartition(tp._1, storage.copy(locationUri = tp._2.asScala.get("location").map(CatalogUtils.stringToURI))))
    catalog.createPartitions(identifier, parts, ignoreIfExists = true)
    catalog.refreshTable(identifier)
    CatalogUtil.updateTableStats(SparkSession.active, this, Some(catalog))
  }

  /**
   since [https://github.com/apache/spark/pull/30624/files#diff-6a78bb6a67f6f99e977982e32e4eb8f5529b725a97bff1152d94cbb83c7c370cR52]
   requireExactMatchedPartitionSpec on V2 drop partition, it's impossible to support partial partition matching drop now.
   */
  override def dropPartitions(idents: Array[InternalRow]): Boolean = {
    val specs = idents.map(ident => delegate.partitionColumnNames.zip(ident.asInstanceOf[GenericInternalRow].values).map(tp => (tp._1, tp._2.toString)).toMap)
    catalog.dropPartitions(identifier, specs.toSeq, ignoreIfNotExists = true, purge = false, retainData = delegate.tableType == CatalogTableType.EXTERNAL)
    catalog.refreshTable(identifier)
    CatalogUtil.updateTableStats(SparkSession.active, this, Some(catalog))
    true
  }

  override def replacePartitionMetadata(ident: InternalRow, properties: jUtil.Map[String, String]): Unit = {
    throw new UnsupportedOperationException("hive not support replace partition command..")
  }

  override def loadPartitionMetadata(ident: InternalRow): jUtil.Map[String, String] = {
    throw new UnsupportedOperationException("v2 hive not support load partition meta")
  }

  override def listPartitionIdentifiers(names: Array[String], ident: InternalRow): Array[InternalRow] = {
    assert(names.length == ident.numFields)
    val partMap = if (names.nonEmpty) Some(names.zip(ident.asInstanceOf[GenericInternalRow].values).map(tp => (tp._1, tp._2.toString)).toMap) else None
    val timeZoneId = CaseInsensitiveMap(storage.properties).get(DateTimeUtils.TIMEZONE_OPTION).getOrElse(SQLConf.get.sessionLocalTimeZone)
    catalog.listPartitions(identifier, partMap).map(cpt => InternalRow.fromSeq(cpt.spec.map(kv => Cast(Literal(kv._2), partitionSchema(kv._1).dataType, Option(timeZoneId)).eval()).toSeq)).toArray
  }

}
