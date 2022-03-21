package com.daasyyds.spark.sql.dsv2.hive.internal

import com.daasyyds.spark.sql.dsv2.hive.V2Table
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogTable, SessionCatalog}
import org.apache.spark.sql.execution.command.CommandUtils.{calculateMultipleLocationSizes, calculateSingleLocationSize}

object CatalogUtil extends Logging {
  /**
    ref org.apache.spark.sql.execution.command.CommandUtils :: [[updateTableStats]]
  */
  def updateTableStats(spark: SparkSession, table: V2Table, externalCatalog: Option[SessionCatalog] = None): Unit = {
    val catalog = externalCatalog.getOrElse(spark.sessionState.catalog)
    if (spark.sessionState.conf.autoSizeUpdateEnabled) {
      val newTable = catalog.getTableMetadata(table.identifier)
      val newSize = calculateTotalSize(spark, newTable)
      val newStats = CatalogStatistics(sizeInBytes = newSize)
      catalog.alterTableStats(table.identifier, Some(newStats))
    } else if (table.stats.nonEmpty) {
      catalog.alterTableStats(table.identifier, None)
    } else {
      // In other cases, we still need to invalidate the table relation cache.
      catalog.refreshTable(table.identifier)
    }
  }

  /**
    ref org.apache.spark.sql.execution.command.CommandUtils :: [[calculateTotalSize]]
   */
  def calculateTotalSize(spark: SparkSession, catalogTable: CatalogTable, externalCatalog: Option[SessionCatalog] = None): BigInt = {
    val catalog = externalCatalog.getOrElse(spark.sessionState.catalog)
    val sessionState = spark.sessionState
    val startTime = System.nanoTime()
    val totalSize = if (catalogTable.partitionColumnNames.isEmpty) {
      calculateSingleLocationSize(sessionState, catalogTable.identifier,
        catalogTable.storage.locationUri)
    } else {
      // Calculate table size as a sum of the visible partitions. See SPARK-21079
      val partitions = catalog.listPartitions(catalogTable.identifier)
      logInfo(s"Starting to calculate sizes for ${partitions.length} partitions.")
      val paths = partitions.map(_.storage.locationUri)
      calculateMultipleLocationSizes(spark, catalogTable.identifier, paths).sum
    }
    logInfo(s"It took ${(System.nanoTime() - startTime) / (1000 * 1000)} ms to calculate" +
      s" the total size for table ${catalogTable.identifier}.")
    totalSize
  }

}
