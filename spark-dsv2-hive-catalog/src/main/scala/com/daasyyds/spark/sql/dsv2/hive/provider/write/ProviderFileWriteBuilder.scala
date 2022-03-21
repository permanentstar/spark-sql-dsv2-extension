package com.daasyyds.spark.sql.dsv2.hive.provider.write

import java.io.IOException
import java.util.UUID

import com.daasyyds.spark.sql.dsv2.hive.V2Table
import com.daasyyds.spark.sql.dsv2.hive.write.HiveFileBatchWrite
import com.daasyyds.spark.sql.dsv2.hive.V2Table
import com.daasyyds.spark.sql.dsv2.hive.write.HiveFileBatchWrite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.{InternalSqlBridge, SparkSession}
import org.apache.spark.sql.ImplicitSqlHelper.StructTypeHelper
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo, SupportsOverwrite, WriteBuilder}
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, DataSource, OutputWriterFactory, WriteJobDescription}
import org.apache.spark.sql.execution.datasources.v2.FileBatchWrite
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters._

/**
  ref org.apache.spark.sql.execution.datasources.v2.FileWriteBuilder
 */
abstract class ProviderFileWriteBuilder(
    paths: Seq[String],
    formatName: String,
    supportsDataType: DataType => Boolean,
    info: LogicalWriteInfo,
    table: V2Table) extends WriteBuilder with SupportsOverwrite with Logging {
  private val schema = info.schema()
  private val queryId = info.queryId()
  private val options = info.options()
  private var overwrite = false

  override def buildForBatch(): BatchWrite = {
    val sparkSession = SparkSession.active
    validateInputs(sparkSession.sessionState.conf.caseSensitiveAnalysis)
    val path = new Path(paths.head)
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val job = getJobInstance(hadoopConf, path)
    val committer = FileCommitProtocol.instantiate(
      sparkSession.sessionState.conf.fileCommitProtocolClass,
      jobId = java.util.UUID.randomUUID().toString,
      outputPath = paths.head)
    lazy val description =
      createWriteJobDescription(sparkSession, hadoopConf, job, paths.head, options.asScala.toMap)
    committer.setupJob(job)
    if (overwrite)  {
      deleteNonPartitionedTableFiles(path.getFileSystem(hadoopConf), path, committer)
    }
    new HiveFileBatchWrite(new FileBatchWrite(job, description, committer), false, Seq.empty, table)
  }

  private def deleteNonPartitionedTableFiles(fs: FileSystem, qualifiedOutputPath: Path, committer: FileCommitProtocol): Unit = {
    if (fs.exists(qualifiedOutputPath) && !committer.deleteWithJob(fs, qualifiedOutputPath, recursive = true)) {
      throw new IOException(s"Unable to clear output " +
        s"directory $qualifiedOutputPath prior to writing to it")
    }
  }

  /**
   * Prepares a write job and returns an [[OutputWriterFactory]].  Client side job preparation can
   * be put here.  For example, user defined output committer can be configured here
   * by setting the output committer class in the conf of spark.sql.sources.outputCommitterClass.
   */
  def prepareWrite(
    sqlConf: SQLConf,
    job: Job,
    options: Map[String, String],
    dataSchema: StructType): OutputWriterFactory

  private def validateInputs(caseSensitiveAnalysis: Boolean): Unit = {
    assert(schema != null, "Missing input data schema")
    assert(queryId != null, "Missing query ID")

    if (paths.length != 1) {
      throw new IllegalArgumentException("Expected exactly one path to be specified, but " +
        s"got: ${paths.mkString(", ")}")
    }
    val pathName = paths.head
    InternalSqlBridge.schemaUtils.checkColumnNameDuplication(schema.fields.map(_.name),
      s"when inserting into $pathName", caseSensitiveAnalysis)
    DataSource.validateSchema(schema)

    schema.foreach { field =>
      if (!supportsDataType(field.dataType)) {
        throw new UnsupportedOperationException(
          s"$formatName data source does not support ${field.dataType.catalogString} data type.")
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
    options: Map[String, String]): WriteJobDescription = {
    val caseInsensitiveOptions = CaseInsensitiveMap(options)
    // Note: prepareWrite has side effect. It sets "job".
    val dataSchema = StructType(schema.fields.take(table.dataSchema.size))
    val outputWriterFactory =
      prepareWrite(sparkSession.sessionState.conf, job, caseInsensitiveOptions, dataSchema)
    val allColumns = table.schema.toAttributes
    val metrics: Map[String, SQLMetric] = BasicWriteJobStatsTracker.metrics
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    val statsTracker = new BasicWriteJobStatsTracker(serializableHadoopConf, metrics)
    new WriteJobDescription(
      uuid = UUID.randomUUID().toString,
      serializableHadoopConf = new SerializableConfiguration(job.getConfiguration),
      outputWriterFactory = outputWriterFactory,
      allColumns = allColumns,
      dataColumns = allColumns.take(table.dataSchema.size),
      partitionColumns = allColumns.takeRight(schema.size - table.dataSchema.size),
      bucketIdExpression = None,
      path = pathName,
      customPartitionLocations = Map.empty,
      maxRecordsPerFile = caseInsensitiveOptions.get("maxRecordsPerFile").map(_.toLong)
        .getOrElse(sparkSession.sessionState.conf.maxRecordsPerFile),
      timeZoneId = caseInsensitiveOptions.get(DateTimeUtils.TIMEZONE_OPTION)
        .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone),
      statsTrackers = Seq(statsTracker)
    )
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    overwrite = true
    this
  }
}
