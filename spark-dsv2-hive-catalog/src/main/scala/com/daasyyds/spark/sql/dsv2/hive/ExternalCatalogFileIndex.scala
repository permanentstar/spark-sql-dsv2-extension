package com.daasyyds.spark.sql.dsv2.hive

import java.net.URI

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

class ExternalCatalogFileIndex(sparkSession: SparkSession,
    catalog: SessionCatalog,
    val table: CatalogTable,
    override val sizeInBytes: Long,
    parameters: Map[String, String],
    userSpecifiedSchema: Option[StructType],
    fileStatusCache: FileStatusCache)
  extends PartitioningAwareFileIndex(sparkSession, parameters, userSpecifiedSchema, fileStatusCache) {

  private val baseLocation: Option[URI] = table.storage.locationUri

  override def partitionSpec(): PartitionSpec = throw new UnsupportedOperationException

  override protected def leafFiles: mutable.LinkedHashMap[Path, FileStatus] = throw new UnsupportedOperationException

  override protected def leafDirToChildrenFiles: Map[Path, Array[FileStatus]] = throw new UnsupportedOperationException

  override def rootPaths: Seq[Path] = baseLocation.map(new Path(_)).toSeq

  override def refresh(): Unit = fileStatusCache.invalidateAll()

  override def partitionSchema: StructType = table.partitionSchema

  override protected def matchPathPattern(file: FileStatus): Boolean = throw new UnsupportedOperationException

  override protected lazy val recursiveFileLookup: Boolean = throw new UnsupportedOperationException

  override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    filterPartitions(partitionFilters).listFiles(Nil, dataFilters)
  }

  override def inputFiles: Array[String] = filterPartitions(Nil).inputFiles

  override def allFiles(): Seq[FileStatus] = throw new UnsupportedOperationException

  override protected def inferPartitioning(): PartitionSpec = throw new UnsupportedOperationException

  def filterPartitions(filters: Seq[Expression]): InMemoryFileIndex = {
    if (table.partitionColumnNames.nonEmpty) {
      val startTime = System.nanoTime()
      val selectedPartitions = catalog.listPartitionsByFilter(
        table.identifier, filters)
      val partitions = selectedPartitions.map { p =>
        val path = new Path(p.location)
        val fs = path.getFileSystem(hadoopConf)
        PartitionPath(
          p.toRow(partitionSchema, sparkSession.sessionState.conf.sessionLocalTimeZone),
          path.makeQualified(fs.getUri, fs.getWorkingDirectory))
      }
      val partitionSpec = PartitionSpec(partitionSchema, partitions)
      val timeNs = System.nanoTime() - startTime
      new InMemoryFileIndex(sparkSession,
        rootPathsSpecified = partitionSpec.partitions.map(_.path),
        parameters = Map.empty,
        userSpecifiedSchema = Some(partitionSpec.partitionColumns),
        fileStatusCache = fileStatusCache,
        userSpecifiedPartitionSpec = Some(partitionSpec),
        metadataOpsTimeNs = Some(timeNs))
    } else {
      new InMemoryFileIndex(sparkSession, rootPaths, parameters = table.storage.properties,
        userSpecifiedSchema = None, fileStatusCache = fileStatusCache)
    }
  }
}
