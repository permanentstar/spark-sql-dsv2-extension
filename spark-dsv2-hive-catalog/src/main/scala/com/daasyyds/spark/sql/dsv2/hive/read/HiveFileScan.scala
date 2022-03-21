package com.daasyyds.spark.sql.dsv2.hive.read

import com.daasyyds.spark.sql.dsv2.hive.V2Table
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.InternalSqlBridge.sessionState
import org.apache.spark.sql.hive.InternalHiveBridge.hiveClientImpl
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

case class HiveFileScan(
    override val sparkSession: SparkSession,
    override val fileIndex: PartitioningAwareFileIndex,
    override val readDataSchema: StructType,
    override val readPartitionSchema: StructType,
    table: V2Table,
    override val partitionFilters: Seq[Expression] = Seq.empty,
    override val dataFilters: Seq[Expression] = Seq.empty) extends FileScan {

  override def withFilters(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan = {
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val hadoopConf = sessionState.newHadoopConf(table.v2Catalog.hadoopConf, sparkSession.sessionState.conf)
    val broadcastedConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    HiveFilePartitionReaderFactory(broadcastedConf, readDataSchema, readPartitionSchema, hiveClientImpl.toHiveTable(table.catalogTable))
  }
}
