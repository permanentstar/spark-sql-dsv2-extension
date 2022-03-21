package com.daasyyds.spark.sql.dsv2.hive.read

import java.io.{Closeable, FileNotFoundException, IOException}
import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.metadata.{Table => HiveTable}
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{FileSplit, JobConf, Reporter, InputFormat => oldInputClass, JobID => oldJobID, RecordReader => oldRecordReader, TaskAttemptID => oldTaskAttemptID, TaskID => oldTaskID}
import org.apache.hadoop.mapreduce.{TaskType, InputFormat => newInputClass, JobID => newJobID, RecordReader => newRecordReader, TaskAttemptID => newTaskAttemptID, TaskID => newTaskID}
import org.apache.hadoop.mapreduce.task.{TaskAttemptContextImpl => newTaskAttemptContextImpl}
import org.apache.hadoop.util.ReflectionUtils
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.ImplicitSqlHelper.{SparkConfHelper, SparkContextHelper, StructTypeHelper}
import org.apache.spark.sql.InternalSqlBridge.{shutdownHookManager, sparkUtil, NextIterator}
import org.apache.spark.sql.InternalSqlBridge.Config.{IGNORE_CORRUPT_FILES, IGNORE_MISSING_FILES}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecificInternalRow, UnsafeRow}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.{PartitionedFile, RecordReaderIterator}
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReaderFactory, PartitionReaderFromIterator, PartitionReaderWithPartitionValues}
import org.apache.spark.sql.hive.InternalHiveBridge.hadoopTableReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

case class HiveFilePartitionReaderFactory(
    broadcastedConf: Broadcast[SerializableConfiguration],
    readDataSchema: StructType,
    partitionSchema: StructType,
    hiveTable: HiveTable) extends FilePartitionReaderFactory with Logging {
  val ss: SparkSession = SparkSession.active
  val ignoreCorruptFiles: Boolean = ss.sparkContext.conf.get(IGNORE_CORRUPT_FILES)
  val ignoreMissingFiles: Boolean = ss.sparkContext.conf.get(IGNORE_MISSING_FILES)

  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {

    val hadoopConf = broadcastedConf.value.value
    val jobConf = new JobConf(hadoopConf)
    val inputFormatClazz = hiveTable.getInputFormatClass
    val reader = if (classOf[newInputClass[_, _]].isAssignableFrom(inputFormatClazz)) {
      newIterableReader(jobConf, file)
    } else {
      oldIterableReader(jobConf, file)
    }
    val iter = if (readDataSchema.isEmpty) {
      val emptyUnsafeRow = new UnsafeRow(0)
      reader.map(_ => emptyUnsafeRow)
    } else {
      val deserializer = sparkUtil.classForName[Deserializer](hiveTable.getSerializationLib).getConstructor().newInstance()
      deserializer.initialize(hadoopConf, hiveTable.getMetadata)
      val soi = deserializer.getObjectInspector.asInstanceOf[StructObjectInspector]
      logDebug(soi.toString)
      val attributes = readDataSchema.toAttributes
      val attrsWithIndex = attributes.zipWithIndex
      val mutableRow = new SpecificInternalRow(attributes.map(_.dataType))
      hadoopTableReader.fillObject(reader.asInstanceOf[Iterator[Writable]], deserializer, attrsWithIndex, mutableRow, deserializer)
    }
    val fileReader = new PartitionReaderFromIterator[InternalRow](iter)
    new PartitionReaderWithPartitionValues(fileReader, readDataSchema,
      partitionSchema, file.partitionValues)
  }

  private def oldIterableReader(jobConf: JobConf, file: PartitionedFile): Iterator[Writable] = {
    def getInputFormat(conf: JobConf): oldInputClass[_, _] = {
      val oldInputFormat = ReflectionUtils.newInstance(hiveTable.getInputFormatClass.asInstanceOf[Class[_]], conf)
        .asInstanceOf[oldInputClass[_, _]]
      oldInputFormat match {
        case c: Configurable => c.setConf(conf)
        case _ =>
      }
      oldInputFormat
    }
    def addLocalConfiguration(conf: JobConf): Unit = {
      Option(TaskContext.get()).foreach(tc => {
        val jobID = new oldJobID(new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(new Date()), tc.stageId())
        val taId = new oldTaskAttemptID(new oldTaskID(jobID, TaskType.MAP, tc.partitionId()), tc.attemptNumber())
        conf.set("mapreduce.task.id", taId.getTaskID.toString)
        conf.set("mapreduce.task.attempt.id", taId.toString)
        conf.setBoolean("mapreduce.task.ismap", true)
        conf.setInt("mapreduce.task.partition", tc.partitionId())
        conf.set("mapreduce.job.id", jobID.toString)
      })
    }
    class IgnoreKeyRecordReaderIterator[V](var reader: oldRecordReader[Any, V]) extends NextIterator[V] with Closeable with Logging {
      private val key = reader.createKey()
      private val value = reader.createValue()
      override def getNext(): V = {
        try {
          finished = !reader.next(key, value)
        } catch {
          case e: FileNotFoundException if ignoreMissingFiles =>
            logWarning(s"Skipped missing file: $file", e)
            finished = true
          // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
          case e: FileNotFoundException if !ignoreMissingFiles => throw e
          case e: IOException if ignoreCorruptFiles =>
            logWarning(s"Skipped the rest content in the corrupted file: $file", e)
            finished = true
        }
        value
      }
      override def close(): Unit = {
        if (reader != null) {
          try {
            reader.close()
          } catch {
            case e: Exception =>
              if (!shutdownHookManager.inShutdown()) {
                logWarning("Exception in RecordReader.close()", e)
              }
          } finally {
            reader = null
          }
        }
      }
    }
    val format = getInputFormat(jobConf)
    val fileSplit = new FileSplit(new Path(new URI(file.filePath)), file.start, file.length, Array.empty[String])
    val _reader = format.getRecordReader(fileSplit, jobConf, Reporter.NULL).asInstanceOf[oldRecordReader[Any, Writable]]
    val reader = new IgnoreKeyRecordReaderIterator(_reader)
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => reader.close()))
    addLocalConfiguration(jobConf)
    reader
  }

  private def newIterableReader(jobConf: JobConf, file: PartitionedFile): Iterator[Writable] = {
    def getInputFormat(conf: JobConf): newInputClass[_, _] = {
      val newInputFormat = ReflectionUtils.newInstance(hiveTable.getInputFormatClass.asInstanceOf[Class[_]], conf)
        .asInstanceOf[newInputClass[_, _]]
      newInputFormat match {
        case c: Configurable => c.setConf(conf)
        case _ =>
      }
      newInputFormat
    }
    val fileSplit = new FileSplit(new Path(new URI(file.filePath)), file.start, file.length, Array.empty[String])
    val attemptId = new newTaskAttemptID(new newTaskID(new newJobID(), TaskType.MAP, 0), 0)
    val hadoopAttemptContext = new newTaskAttemptContextImpl(jobConf, attemptId)
    val reader = {
      val _reader = getInputFormat(jobConf).createRecordReader(fileSplit, hadoopAttemptContext).asInstanceOf[newRecordReader[Any, Writable]]
      _reader.initialize(fileSplit, hadoopAttemptContext)
      new RecordReaderIterator(_reader)
    }
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => reader.close()))
    reader
  }

}
