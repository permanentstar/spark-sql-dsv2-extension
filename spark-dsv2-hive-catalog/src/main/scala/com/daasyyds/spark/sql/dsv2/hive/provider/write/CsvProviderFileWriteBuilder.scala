package com.daasyyds.spark.sql.dsv2.hive.provider.write

import com.daasyyds.spark.sql.dsv2.hive.V2Table
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.sql.InternalSqlBridge.{AtomicType, UserDefinedType}
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.execution.datasources.csv.CsvOutputWriter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}

class CsvProviderFileWriteBuilder(
    paths: Seq[String],
    formatName: String,
    supportsDataType: DataType => Boolean,
    info: LogicalWriteInfo,
    table: V2Table) extends ProviderFileWriteBuilder(paths, formatName, supportsDataType, info, table) {

  override def prepareWrite(sqlConf: SQLConf, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = {
    val conf = job.getConfiguration
    val csvOptions = new CSVOptions(
      options,
      columnPruning = sqlConf.csvColumnPruning,
      sqlConf.sessionLocalTimeZone)
    csvOptions.compressionCodec.foreach { codec =>
      CompressionCodecs.setCodecConfiguration(conf, codec)
    }

    new OutputWriterFactory {
      override def newInstance(
        path: String,
        dataSchema: StructType,
        context: TaskAttemptContext): OutputWriter = {
        new CsvOutputWriter(path, dataSchema, context, csvOptions)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".csv" + CodecStreams.getCompressionExtension(context)
      }
    }
  }
}

object CsvProviderFileWriteBuilder {
  def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: AtomicType => true
    case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)
    case _ => false
  }
}
