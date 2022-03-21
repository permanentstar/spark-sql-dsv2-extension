package com.daasyyds.spark.sql.dsv2.hive.write

import com.daasyyds.spark.sql.dsv2.hive.internal.CatalogUtil
import com.daasyyds.spark.sql.dsv2.hive.V2Table
import org.apache.spark.internal.Logging
import org.apache.spark.sql.InternalSqlBridge.partitioningUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogTablePartition}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.execution.command.CommandUtils
import org.apache.spark.sql.execution.datasources.{PartitioningUtils, WriteTaskResult}
import org.apache.spark.sql.execution.datasources.v2.FileBatchWrite
import org.apache.spark.sql.internal.SQLConf

class HiveFileBatchWrite(fileBatchWrite: FileBatchWrite, partitionsTrackedByCatalog: Boolean, initialMatchingPartitions: Seq[TablePartitionSpec], table: V2Table) extends BatchWrite with Logging {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = fileBatchWrite.createBatchWriterFactory(info)

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    fileBatchWrite.commit(messages)
    commitMetastore(messages)
  }

  override def useCommitCoordinator(): Boolean = false

  /**
    ref v1 implement
    org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand :: run
  */

    // since dynamicPartitionOverwrite is default enable in hive datasource, we need not to drop any partition here
  private def commitMetastore(messages: Array[WriterCommitMessage]): Unit = {
    val ss = SparkSession.active

    val partitions = messages.filter(msg => msg.isInstanceOf[WriteTaskResult]).map(msg => msg.asInstanceOf[WriteTaskResult].summary.updatedPartitions).reduceOption(_ ++ _).getOrElse(Set.empty)
    if (partitions.nonEmpty && partitionsTrackedByCatalog) {
      val updatedPartitions = partitions.map(PartitioningUtils.parsePathFragment)
      val newPartitions = updatedPartitions -- initialMatchingPartitions
      if (newPartitions.nonEmpty) {
        val parts = newPartitions.toSeq.map(spec => {
          val normalizedSpec = partitioningUtils.normalizePartitionSpec(
            spec,
            table.partitionSchema,
            table.identifier.quotedString,
            ss.sessionState.conf.resolver)
          CatalogTablePartition(normalizedSpec, table.storage.copy(
            locationUri = None))
        })
        val batchSize = SQLConf.get.getConf(SQLConf.ADD_PARTITION_BATCH_SIZE)
        parts.toIterator.grouped(batchSize).foreach { batch =>
          table.catalog.createPartitions(table.identifier, batch, ignoreIfExists = true)
        }
        table.catalog.refreshTable(table.identifier)
        if (table.stats.nonEmpty && ss.sessionState.conf.autoSizeUpdateEnabled) {
          // Updating table stats only if new partition is not empty
          val addedSize = CommandUtils.calculateMultipleLocationSizes(ss, table.identifier, parts.map(_.storage.locationUri)).sum
          if (addedSize > 0) {
            table.catalog.alterTableStats(table.identifier, Some(CatalogStatistics(sizeInBytes = table.stats.get.sizeInBytes + addedSize)))
          }
        } else {
          // since we updateTableStats at last, we do nothing here
        }
      }
    }
    CatalogUtil.updateTableStats(ss, table, Some(table.catalog))
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = fileBatchWrite.abort(messages)


}
