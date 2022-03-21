package com.daasyyds.spark.sql.dsv2.hive.write

import java.io.IOException
import java.util.{Locale, UUID}

import com.daasyyds.spark.sql.dsv2.hive.V2Table
import com.daasyyds.spark.sql.dsv2.hive.V2Table
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.metadata.{Table => HiveTable}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.{InternalSqlBridge, SparkSession}
import org.apache.spark.sql.ImplicitSqlHelper.StructTypeHelper
import org.apache.spark.sql.InternalSqlBridge.sessionState
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, DataSource, PartitioningUtils, WriteJobDescription}
import org.apache.spark.sql.execution.datasources.v2.FileBatchWrite
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.InternalHiveBridge.FileSinkDesc
import org.apache.spark.sql.hive.execution.{HiveFileFormat, HiveOptions}
import org.apache.spark.sql.sources.{AlwaysTrue, And, EqualNullSafe, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters._

class HiveFileFormatWriteBuilder(
      paths: Seq[String],
      hiveTable: HiveTable,
      info: LogicalWriteInfo,
      table: V2Table) extends WriteBuilder with SupportsOverwrite with SupportsDynamicOverwrite with Logging {

  private val allColumns = info.schema().toAttributes
  private val dataColumns = allColumns.take(allColumns.length - hiveTable.getPartCols.size())
  private val partColumns = allColumns.takeRight(hiveTable.getPartCols.size())
  private val options = info.options()
  val ss: SparkSession = SparkSession.active
  val hadoopConf: Configuration = sessionState.newHadoopConf(table.v2Catalog.hadoopConf, ss.sessionState.conf)
  private lazy val hiveFileFormat = new HiveFileFormat(determineCompress(new FileSinkDesc(paths.head, new TableDesc(hiveTable.getInputFormatClass, hiveTable.getOutputFormatClass, hiveTable.getMetadata), false)))
  private var staticPartitions: TablePartitionSpec = Map.empty
  private var dynamicPartitionOverwrite = false
  private var overwrite = false
  private val partitionsTrackedByCatalog = ss.sessionState.conf.manageFilesourcePartitions && table.partitionSchema.nonEmpty && table.tracksPartitionsInCatalog

  validateInputs(ss.sessionState.conf.caseSensitiveAnalysis)

  override def buildForBatch(): BatchWrite = {
    val path = new Path(paths.head)
    val fs = path.getFileSystem(hadoopConf)
    val qualifiedOutputPath = path.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val jobId = java.util.UUID.randomUUID().toString
    val committerOutputPath = if (dynamicPartitionOverwrite) {
      FileCommitProtocol.getStagingDir(path.toString, jobId).makeQualified(fs.getUri, fs.getWorkingDirectory)
    } else {
      qualifiedOutputPath
    }
    val job = getJobInstance(hadoopConf, committerOutputPath)
    val committer = FileCommitProtocol.instantiate(
      ss.sessionState.conf.fileCommitProtocolClass,
      jobId,
      outputPath = paths.head,
      dynamicPartitionOverwrite)
    committer.setupJob(job)

    var customPartitionLocations: Map[TablePartitionSpec, String] = Map.empty
    var initialMatchingPartitions: Seq[TablePartitionSpec] = Nil
    if (partitionsTrackedByCatalog) {
      val matchingPartitions = table.catalog.listPartitions(table.identifier, Some(staticPartitions))
      initialMatchingPartitions = matchingPartitions.map(_.spec)
      customPartitionLocations = getCustomPartitionLocations(fs, table.catalogTable, qualifiedOutputPath, matchingPartitions)
    }
    lazy val description = createWriteJobDescription(ss, hadoopConf, job, committerOutputPath.toString, customPartitionLocations, options.asScala.toMap)
    if (overwrite && !dynamicPartitionOverwrite) { // non partitioned table overwrite
      deleteNonPartitionedTableFiles(fs, qualifiedOutputPath, committer)
    }
    new HiveFileBatchWrite(new FileBatchWrite(job, description, committer), partitionsTrackedByCatalog, initialMatchingPartitions, table)
  }

  private def getCustomPartitionLocations(
     fs: FileSystem,
     table: CatalogTable,
     qualifiedOutputPath: Path,
     partitions: Seq[CatalogTablePartition]): Map[TablePartitionSpec, String] = {
    partitions.flatMap { p =>
      val defaultLocation = qualifiedOutputPath.suffix(
        "/" + PartitioningUtils.getPathFragment(p.spec, table.partitionSchema)).toString
      val catalogLocation = new Path(p.location).makeQualified(
        fs.getUri, fs.getWorkingDirectory).toString
      if (catalogLocation != defaultLocation) {
        Some(p.spec -> catalogLocation)
      } else {
        None
      }
    }.toMap
  }

  private def deleteNonPartitionedTableFiles(fs: FileSystem, qualifiedOutputPath: Path, committer: FileCommitProtocol): Unit = {
    if (fs.exists(qualifiedOutputPath) && !committer.deleteWithJob(fs, qualifiedOutputPath, recursive = true)) {
      throw new IOException(s"Unable to clear output " +
        s"directory $qualifiedOutputPath prior to writing to it")
    }
  }

  private def validateInputs(caseSensitiveAnalysis: Boolean): Unit = {
    assert(info.schema() != null, "Missing input data schema")

    if (paths.length != 1) {
      throw new IllegalArgumentException("Expected exactly one path to be specified, but " +
        s"got: ${paths.mkString(", ")}")
    }
    val pathName = paths.head
    InternalSqlBridge.schemaUtils.checkColumnNameDuplication(info.schema().fields.map(_.name),
      s"when inserting into $pathName", caseSensitiveAnalysis)
    DataSource.validateSchema(info.schema())

    if (hiveTable.getBucketCols.size() > 0) {
      val enforceBucketingConfig = "hive.enforce.bucketing"
      val enforceSortingConfig = "hive.enforce.sorting"
      val message = s"Output Hive table ${table.identifier} is bucketed but Spark " +
        "currently does NOT populate bucketed output which is compatible with Hive."
      if (hadoopConf.get(enforceBucketingConfig, "true").toBoolean ||
        hadoopConf.get(enforceSortingConfig, "true").toBoolean) {
        throw new UnsupportedOperationException(message)
      } else {
        logWarning(message + s" Inserting data anyways since both $enforceBucketingConfig and " +
          s"$enforceSortingConfig are set to false.")
      }
    }
  }

  private def getJobInstance(hadoopConf: Configuration, path: Path): Job = {
    val job = Job.getInstance(hadoopConf)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])
    FileOutputFormat.setOutputPath(job, path)
    job
  }

  private def createWriteJobDescription(
    sparkSession: SparkSession,
    hadoopConf: Configuration,
    job: Job,
    pathName: String,
    customPartitionLocations: Map[TablePartitionSpec, String],
    options: Map[String, String]): WriteJobDescription = {
    val caseInsensitiveOptions = CaseInsensitiveMap(options)
    // Note: prepareWrite has side effect. It sets "job".
    val dataSchema = StructType(info.schema().fields.take(dataColumns.length))
    val outputWriterFactory = hiveFileFormat.prepareWrite(sparkSession, job, options, dataSchema)
    val metrics: Map[String, SQLMetric] = BasicWriteJobStatsTracker.metrics
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    val statsTracker = new BasicWriteJobStatsTracker(serializableHadoopConf, metrics)
    new WriteJobDescription(
      uuid = UUID.randomUUID().toString,
      serializableHadoopConf = new SerializableConfiguration(job.getConfiguration),
      outputWriterFactory = outputWriterFactory,
      allColumns = allColumns,
      dataColumns = dataColumns,
      partitionColumns = partColumns,
      bucketIdExpression = None,
      path = pathName,
      customPartitionLocations = customPartitionLocations,
      maxRecordsPerFile = caseInsensitiveOptions.get("maxRecordsPerFile").map(_.toLong)
        .getOrElse(sparkSession.sessionState.conf.maxRecordsPerFile),
      timeZoneId = caseInsensitiveOptions.get(DateTimeUtils.TIMEZONE_OPTION)
        .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone),
      statsTrackers = Seq(statsTracker)
    )
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    // filters here does not affect data write but only affect job commit when relate to static partitions
    if (filters.length == 1 && filters(0) == AlwaysTrue) {
      // do nothing
    } else {
      // before https://issues.apache.org/jira/browse/SPARK-36706 resolved, we must dedup ourself
      staticPartitions = mergeDuplicateFilters(filters)
    }
    overwriteDynamicPartitions() // hive datasource always enable dynamicPartitionOverwrite
  }

  private def mergeDuplicateFilters(filters: Array[Filter]): TablePartitionSpec = {
    filters.map(unwrapDeleteExpressions).reduce((m1, m2) => m1 ++ m2)
  }

  private def unwrapDeleteExpressions(filter: Filter): TablePartitionSpec = {
    filter match {
      case And(l, r) => unwrapDeleteExpressions(l) ++ unwrapDeleteExpressions(r)
      case EqualNullSafe(att, value) => Map(att -> value.toString)
      case _ => throw new UnsupportedOperationException(s"unexpect filter $filter")
    }
  }

  override def overwriteDynamicPartitions(): WriteBuilder = {
    overwrite = true
    dynamicPartitionOverwrite = partColumns.nonEmpty
    this
  }

  private def determineCompress(fsd: FileSinkDesc): FileSinkDesc = {
    val isCompressed = fsd.getTableInfo.getOutputFileFormatClassName.toLowerCase(Locale.ROOT) match {
      case formatName if formatName.endsWith("orcoutputformat") => false
      case _ => hadoopConf.get("hive.exec.compress.output", "false").toBoolean
    }
    if (isCompressed) {
      hadoopConf.set("mapreduce.output.fileoutputformat.compress", "true")
      fsd.setCompressed(true)
      fsd.setCompressCodec(hadoopConf.get("mapreduce.output.fileoutputformat.compress.codec"))
      fsd.setCompressType(hadoopConf.get("mapreduce.output.fileoutputformat.compress.type"))
    } else {
      HiveOptions.getHiveWriteCompression(fsd.getTableInfo, ss.sessionState.conf)
        .foreach { case (compression, codec) => hadoopConf.set(compression, codec) }
    }
    fsd
  }
}