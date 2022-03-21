package com.daasyyds.spark.sql.dsv2.hive.provider.write

import com.daasyyds.spark.sql.dsv2.hive.V2Table
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.sql.InternalSqlBridge
import org.apache.spark.sql.InternalSqlBridge.{AtomicType, UserDefinedType}
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.execution.datasources.json.JsonOutputWriter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class JsonProviderFileWriteBuilder(
    paths: Seq[String],
    formatName: String,
    supportsDataType: DataType => Boolean,
    info: LogicalWriteInfo,
    table: V2Table) extends ProviderFileWriteBuilder(paths, formatName, supportsDataType, info, table) {

  override def prepareWrite(sqlConf: SQLConf, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = {
    val conf = job.getConfiguration
    val parsedOptions = new InternalSqlBridge.JSONOptions(
      options,
      sqlConf.sessionLocalTimeZone,
      sqlConf.columnNameOfCorruptRecord)
    parsedOptions.compressionCodec.foreach { codec =>
      CompressionCodecs.setCodecConfiguration(conf, codec)
    }

    new OutputWriterFactory {
      override def newInstance(
        path: String,
        dataSchema: StructType,
        context: TaskAttemptContext): OutputWriter = {
        new JsonOutputWriter(path, parsedOptions, dataSchema, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".json" + CodecStreams.getCompressionExtension(context)
      }
    }
  }
}

object JsonProviderFileWriteBuilder {
  def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: AtomicType => true
    case st: StructType => st.forall { f => supportsDataType(f.dataType) }
    case ArrayType(elementType, _) => supportsDataType(elementType)
    case MapType(keyType, valueType, _) =>
      supportsDataType(keyType) && supportsDataType(valueType)
    case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)
    case _: NullType => true
    case _ => false
  }
}
