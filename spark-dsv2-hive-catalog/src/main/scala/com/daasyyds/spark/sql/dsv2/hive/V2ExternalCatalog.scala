package com.daasyyds.spark.sql.dsv2.hive

import java.{util => jUtil}

import com.daasyyds.spark.sql.dsv2.hive.internal.ConfHelper._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.ImplicitSqlHelper.{CatalogDatabaseHelper, TableIdentifierHelper}
import org.apache.spark.sql.ImplicitSqlHelper.catalogV2Implicits.NamespaceHelper
import org.apache.spark.sql.InternalSqlBridge.{catalogV2Util, CatalogTransform}
import org.apache.spark.sql.hive.InternalHiveBridge.{HiveExternalCatalog, HiveSessionCatalog}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils, SessionCatalog => V2Catalog}
import org.apache.spark.sql.catalyst.plans.logical.{FormatClasses, SerdeInfo}
import org.apache.spark.sql.conf.V2SqlConf
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.NamespaceChange.RemoveProperty
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import scala.collection.mutable

class V2ExternalCatalog extends TableCatalog with SupportsNamespaces with SQLConfHelper with Logging {
  private var catalog: V2Catalog = _
  private var catalogName: String = _
  private var catalogHadoopConf: Configuration = _
  private var catalogSparkConf: SparkConf = _
  override val defaultNamespace: Array[String] = Array("default")

  override lazy val name: String = {
    require(catalogName != null, "The External table catalog is not initialed")
    catalogName
  }

  lazy val hadoopConf: Configuration = {
    require(catalogHadoopConf != null, "The External table catalog is not initialed")
    catalogHadoopConf
  }

  lazy val sparkConf: SparkConf = {
    require(catalogSparkConf != null, "The External table catalog is not initialed")
    catalogSparkConf
  }

  def sessionCatalog(): V2Catalog = catalog

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    assert(catalogName == null, "The External table catalog is already initialed")
    catalogName = name
    val ss = SparkSession.active
    implicit val (sparkConf: SparkConf, hadoopConf: Configuration) = {
      val conf = V2SqlConf(CATALOG_TYPE).folk(options.asScala.toMap)
      var newHadoopConf = new Configuration(false)
      conf.getConf(HIVE_SITE_FILE) match {
        case "classpath" => newHadoopConf.addResource(checkAndGetHiveFileClasspath(conf.getConf(HIVE_SITE_FILE_CLASSPATH, checkExist = true).get))
        case "path" => newHadoopConf.addResource(new Path(checkAndGetHiveFilePath(conf.getConf(HIVE_SITE_FILE_PATH, checkExist = true).get)))
      }
      newHadoopConf = ss.sessionState.newHadoopConfWithOptions(newHadoopConf.asScala.map(kv => (kv.getKey, kv.getValue)).toMap)
      val sparkConfClone = ss.sparkContext.getConf.clone()
      sparkConfClone.setAll(conf.getAllConf.filter(_._1.toLowerCase.startsWith("spark.")))
      sparkConfClone.set(StaticSQLConf.WAREHOUSE_PATH.key, newHadoopConf.get("hive.metastore.warehouse.dir"))
      catalogHadoopConf = newHadoopConf
      catalogSparkConf = sparkConfClone
      (sparkConfClone, newHadoopConf)
    }
    catalog = new HiveSessionCatalog(
      externalCatalogBuilder(),
      () => ss.sharedState.globalTempViewManager,
      null,
      null,
      hadoopConf,
      null,
      ss.sessionState.resourceLoader
    )
  }

  private def externalCatalogBuilder()(implicit sparkConf: SparkConf, hadoopConf: Configuration): () => HiveExternalCatalog = {
    () => {
      lazy val catalog = {
        new HiveExternalCatalog(sparkConf, hadoopConf)
      }
      catalog
    }
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    namespace match {
      case Array(db) =>
        catalog
          .listTables(db)
          .map(ident => Identifier.of(Array(ident.database.getOrElse("")), ident.table))
          .toArray
      case _ =>
        throw new NoSuchNamespaceException(namespace)
    }
  }

  override def loadTable(ident: Identifier): Table = {
    val catalogTable = try {
      catalog.getTableMetadata(ident.asTableIdentifier)
    } catch {
      case _: NoSuchTableException =>
        throw new NoSuchTableException(ident)
    }
    new V2Table(name, this, catalogTable)
  }

  override def invalidateTable(ident: Identifier): Unit = catalog.refreshTable(ident.asTableIdentifier)

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: jUtil.Map[String, String]): Table = {
    def getStorageFormatAndProvider(
        provider: Option[String],
        options: Map[String, String],
        location: Option[String],
        maybeSerdeInfo: Option[SerdeInfo]): (CatalogStorageFormat, String) = {
      val nonHiveStorageFormat = CatalogStorageFormat.empty.copy(locationUri = location.map(CatalogUtils.stringToURI), properties = options)
      val defaultHiveStorage = HiveSerDe.getDefaultStorage(conf).copy(locationUri = location.map(CatalogUtils.stringToURI), properties = options)
      if (provider.isDefined) {
        (nonHiveStorageFormat, provider.get)
      } else if (maybeSerdeInfo.isDefined) {
        val serdeInfo = maybeSerdeInfo.get
        SerdeInfo.checkSerdePropMerging(serdeInfo.serdeProperties, defaultHiveStorage.properties)
        val storageFormat = if (serdeInfo.storedAs.isDefined) {
          // If `STORED AS fileFormat` is used, infer inputFormat, outputFormat and serde from it.
          HiveSerDe.sourceToSerDe(serdeInfo.storedAs.get) match {
            case Some(hiveSerde) =>
              defaultHiveStorage.copy(
                inputFormat = hiveSerde.inputFormat.orElse(defaultHiveStorage.inputFormat),
                outputFormat = hiveSerde.outputFormat.orElse(defaultHiveStorage.outputFormat),
                // User specified serde takes precedence over the one inferred from file format.
                serde = serdeInfo.serde.orElse(hiveSerde.serde).orElse(defaultHiveStorage.serde),
                properties = serdeInfo.serdeProperties ++ defaultHiveStorage.properties)
            case _ => throw new UnsupportedOperationException(s"STORED AS with file format '${serdeInfo.storedAs.get}' is invalid.")
          }
        } else {
          defaultHiveStorage.copy(
            inputFormat = serdeInfo.formatClasses.map(_.input).orElse(defaultHiveStorage.inputFormat),
            outputFormat = serdeInfo.formatClasses.map(_.output).orElse(defaultHiveStorage.outputFormat),
            serde = serdeInfo.serde.orElse(defaultHiveStorage.serde),
            properties = serdeInfo.serdeProperties ++ defaultHiveStorage.properties)
        }
        (storageFormat, DDLUtils.HIVE_PROVIDER)
      } else {
        if (!conf.getConf(SQLConf.LEGACY_CREATE_HIVE_TABLE_BY_DEFAULT)) {
          (nonHiveStorageFormat, conf.defaultDataSourceName)
        } else {
          logWarning("A Hive serde table will be created as there is no table provider " +
            s"specified. You can set ${SQLConf.LEGACY_CREATE_HIVE_TABLE_BY_DEFAULT.key} to false " +
            "so that native data source table will be created instead.")
          (defaultHiveStorage, DDLUtils.HIVE_PROVIDER)
        }
      }
    }
    def convertToSerde(options: mutable.Map[String, String]): Option[SerdeInfo] = {
      val formatClz = if (options.contains("hive.input-format") && options.contains("hive.output-format")) {
        Some(FormatClasses(options.remove("hive.input-format").get, options.remove("hive.output-format").get))
      } else None
      val storedAs = options.remove("hive.stored-as")
      val serde = options.remove("hive.serde")
      val serdeProperteies = options.filter(_._1.startsWith(TableCatalog.OPTION_PREFIX))
      serdeProperteies.foreach(p => options.remove(p._1))
      if (formatClz.isDefined || storedAs.isDefined || serde.isDefined) {
        Some(SerdeInfo(storedAs, formatClz, serde, serdeProperteies.map(kv => (kv._1.substring(TableCatalog.OPTION_PREFIX.length), kv._2)).toMap))
      } else None
    }
    val tableProperties = mutable.HashMap(properties.asScala.toSeq:_*)
    val prd = tableProperties.remove(TableCatalog.PROP_PROVIDER)
    val location = tableProperties.remove(TableCatalog.PROP_LOCATION)
    val serde = convertToSerde(tableProperties)
    val comment = tableProperties.remove(TableCatalog.PROP_COMMENT)
    val (storageFormat, provider) = getStorageFormatAndProvider(prd, tableProperties.toMap, location, serde)
    val (partitionColumns, maybeBucketSpec) = CatalogTransform.convertTransforms(partitions)
    val tableType = if (location.isDefined) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED
    val tableDesc = CatalogTable(
      identifier = ident.asTableIdentifier,
      tableType = tableType,
      storage = storageFormat,
      schema = schema,
      provider = Some(provider),
      partitionColumnNames = partitionColumns,
      bucketSpec = maybeBucketSpec,
      properties = tableProperties.toMap,
      tracksPartitionsInCatalog = conf.manageFilesourcePartitions,
      comment = comment)

    try {
      catalog.createTable(tableDesc, ignoreIfExists = false)
    } catch {
      case _: TableAlreadyExistsException =>
        throw new TableAlreadyExistsException(ident)
    }

    loadTable(ident)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val catalogTable = try {
      catalog.getTableMetadata(ident.asTableIdentifier)
    } catch {
      case _: NoSuchTableException =>
        throw new NoSuchTableException(ident)
    }

    val properties = catalogV2Util.applyPropertiesChanges(catalogTable.properties, changes)
    val schema = catalogV2Util.applySchemaChanges(catalogTable.schema, changes)
    val comment = properties.get(TableCatalog.PROP_COMMENT)
    val owner = properties.getOrElse(TableCatalog.PROP_OWNER, catalogTable.owner)
    val location = properties.get(TableCatalog.PROP_LOCATION).map(CatalogUtils.stringToURI)
    val storage = if (location.isDefined) {
      catalogTable.storage.copy(locationUri = location)
    } else {
      catalogTable.storage
    }

    try {
      catalog.alterTable(
        catalogTable.copy(
          properties = properties, schema = schema, owner = owner, comment = comment,
          storage = storage))
    } catch {
      case _: NoSuchTableException =>
        throw new NoSuchTableException(ident)
    }

    loadTable(ident)
  }

  override def dropTable(ident: Identifier): Boolean = {
    try {
      if (loadTable(ident) != null) {
        catalog.dropTable(
          ident.asTableIdentifier,
          ignoreIfNotExists = true,
          purge = true /* skip HDFS trash */)
        true
      } else {
        false
      }
    } catch {
      case _: NoSuchTableException =>
        false
    }
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    if (tableExists(newIdent)) {
      throw new TableAlreadyExistsException(newIdent)
    }

    // Load table to make sure the table exists
    loadTable(oldIdent)
    catalog.renameTable(oldIdent.asTableIdentifier, newIdent.asTableIdentifier)
  }

  override def namespaceExists(namespace: Array[String]): Boolean = namespace match {
    case Array(db) =>
      catalog.databaseExists(db)
    case _ =>
      false
  }

  override def listNamespaces(): Array[Array[String]] = {
    catalog.listDatabases().map(Array(_)).toArray
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    namespace match {
      case Array() =>
        listNamespaces()
      case Array(db) if catalog.databaseExists(db) =>
        Array()
      case _ =>
        throw new NoSuchNamespaceException(namespace)
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): jUtil.Map[String, String] =  {
    namespace match {
      case Array(db) =>
        catalog.getDatabaseMetadata(db).toMetadata

      case _ =>
        throw new NoSuchNamespaceException(namespace)
    }
  }

  override def createNamespace(namespace: Array[String], metadata: jUtil.Map[String, String]): Unit = namespace match {
    case Array(db) if !catalog.databaseExists(db) =>
      catalog.createDatabase(
        CatalogTransform.toCatalogDatabase(db, metadata, defaultLocation = Some(catalog.getDefaultDBPath(db))),
        ignoreIfExists = false)

    case Array(_) =>
      throw new NamespaceAlreadyExistsException(namespace)

    case _ =>
      throw new IllegalArgumentException(s"Invalid namespace name: ${namespace.quoted}")
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    namespace match {
      case Array(db) =>
        // validate that this catalog's reserved properties are not removed
        changes.foreach {
          case remove: RemoveProperty
            if catalogV2Util.NAMESPACE_RESERVED_PROPERTIES.contains(remove.property) =>
            throw new UnsupportedOperationException(
              s"Cannot remove reserved property: ${remove.property}")
          case _ =>
        }

        val metadata = catalog.getDatabaseMetadata(db).toMetadata
        catalog.alterDatabase(
          CatalogTransform.toCatalogDatabase(db, catalogV2Util.applyNamespaceChanges(metadata, changes)))

      case _ =>
        throw new NoSuchNamespaceException(namespace)
    }
  }

  override def dropNamespace(namespace: Array[String]): Boolean = namespace match {
    case Array(db) if catalog.databaseExists(db) =>
      if (catalog.listTables(db).nonEmpty) {
        throw new IllegalStateException(s"Namespace ${namespace.quoted} is not empty")
      }
      catalog.dropDatabase(db, ignoreIfNotExists = false, cascade = false)
      true

    case Array(_) =>
      // exists returned false
      false

    case _ =>
      throw new NoSuchNamespaceException(namespace)
  }

  override def toString: String = s"V2ExternalCatalog($name)"
}
