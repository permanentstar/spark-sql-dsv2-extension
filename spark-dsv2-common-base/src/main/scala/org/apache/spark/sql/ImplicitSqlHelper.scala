package org.apache.spark.sql

import java.{util => jUtil}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.connector.catalog.{CatalogV2Implicits, Identifier, SupportsNamespaces}
import org.apache.spark.sql.types.{DataType, StructType}

import scala.collection.JavaConverters._
import scala.collection.mutable


object ImplicitSqlHelper {
  val catalogV2Implicits: CatalogV2Implicits.type = CatalogV2Implicits
  implicit class TableIdentifierHelper(ident: Identifier) {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper

    def asTableIdentifier: TableIdentifier = {
      ident.namespace match {
        case Array(db) =>
          TableIdentifier(ident.name, Some(db))
        case _ =>
          throw new NoSuchTableException(
            s"V2 session catalog requires a single-part namespace: ${ident.quoted}")
      }
    }
  }

  implicit class CatalogDatabaseHelper(catalogDatabase: CatalogDatabase) {
    def toMetadata: jUtil.Map[String, String] = {
      val metadata = mutable.HashMap[String, String]()

      catalogDatabase.properties.foreach {
        case (key, value) => metadata.put(key, value)
      }
      metadata.put(SupportsNamespaces.PROP_LOCATION, catalogDatabase.locationUri.toString)
      metadata.put(SupportsNamespaces.PROP_COMMENT, catalogDatabase.description)

      metadata.asJava
    }
  }

  implicit class DataTypeHelper(dataType: DataType) {
    def asNullable: DataType = dataType.asNullable
  }

  implicit class StructTypeHelper(structType: StructType) {
    def asNullable: StructType = structType.asNullable
    def toAttributes: Seq[AttributeReference] = structType.toAttributes
  }

  implicit class SparkConfHelper(conf: SparkConf) {
    def get[T](entry: ConfigEntry[T]): T = conf.get(entry)
  }

  implicit class SparkContextHelper(context: SparkContext) {
    def conf: SparkConf = context.conf
  }
}
