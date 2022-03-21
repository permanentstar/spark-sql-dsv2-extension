package org.apache.spark.sql

import java.net.URI
import java.{util => jUtil}

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogDatabase, CatalogUtils}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, SupportsNamespaces}
import org.apache.spark.sql.connector.expressions.{BucketTransform, FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.util.{PartitioningUtils, SchemaUtils}
import org.apache.spark.util.{ShutdownHookManager, Utils}

import scala.collection.JavaConverters._
import scala.collection.mutable

object InternalSqlBridge {
  object CatalogTransform {
    def convertTransforms(partitions: Seq[Transform]): (Seq[String], Option[BucketSpec]) = {
      val identityCols = new mutable.ArrayBuffer[String]
      var bucketSpec = Option.empty[BucketSpec]

      partitions.map {
        case IdentityTransform(FieldReference(Seq(col))) =>
          identityCols += col

        case BucketTransform(numBuckets, FieldReference(Seq(col))) =>
          bucketSpec = Some(BucketSpec(numBuckets, col :: Nil, Nil))

        case transform =>
          throw new UnsupportedOperationException(
            s"SessionCatalog does not support partition transform: $transform")
      }

      (identityCols, bucketSpec)
    }

    def toCatalogDatabase(
                           db: String,
                           metadata: jUtil.Map[String, String],
                           defaultLocation: Option[URI] = None): CatalogDatabase = {
      CatalogDatabase(
        name = db,
        description = metadata.getOrDefault(SupportsNamespaces.PROP_COMMENT, ""),
        locationUri = Option(metadata.get(SupportsNamespaces.PROP_LOCATION))
          .map(CatalogUtils.stringToURI)
          .orElse(defaultLocation)
          .getOrElse(throw new IllegalArgumentException("Missing database location")),
        properties = metadata.asScala.toMap --
          Seq(SupportsNamespaces.PROP_COMMENT, SupportsNamespaces.PROP_LOCATION))
    }
  }
  val catalogV2Util: CatalogV2Util.type = CatalogV2Util

  type V1Table = org.apache.spark.sql.connector.catalog.V1Table
  type AtomicType = org.apache.spark.sql.types.AtomicType
  type UserDefinedType[U>: Null] = org.apache.spark.sql.types.UserDefinedType[U]
  type OrcOutputWriter = org.apache.spark.sql.execution.datasources.orc.OrcOutputWriter
  val schemaUtils: SchemaUtils.type = org.apache.spark.sql.util.SchemaUtils
  val orcFileFormat: OrcFileFormat.type = org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
  val partitioningUtils: PartitioningUtils.type = org.apache.spark.sql.util.PartitioningUtils
  val sparkHadoopUtil: SparkHadoopUtil.type  = org.apache.spark.deploy.SparkHadoopUtil
  type JSONOptions = org.apache.spark.sql.catalyst.json.JSONOptions
  val sparkUtil: Utils.type = org.apache.spark.util.Utils
  type NextIterator[U] = org.apache.spark.util.NextIterator[U]
  val shutdownHookManager: ShutdownHookManager.type = org.apache.spark.util.ShutdownHookManager


  object Config {
    val IGNORE_MISSING_FILES: ConfigEntry[Boolean] = org.apache.spark.internal.config.IGNORE_MISSING_FILES
    val IGNORE_CORRUPT_FILES: ConfigEntry[Boolean] = org.apache.spark.internal.config.IGNORE_CORRUPT_FILES
  }
  val sessionState: SessionState.type = org.apache.spark.sql.internal.SessionState

}
