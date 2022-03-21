package org.apache.spark.sql.conf

import java.util.concurrent.{ConcurrentHashMap => jConcurrentHashMap}

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry, ConfigReader}

import scala.collection.JavaConverters._
import scala.collection.immutable

class V2SqlConf private(v2SqlConfEntries :jConcurrentHashMap[String, ConfigEntry[_]], readOnly: Boolean = false) {
  private def this() = {
    this(new jConcurrentHashMap[String, ConfigEntry[_]](), true)
  }
  @transient private val settings = new jConcurrentHashMap[String, String]()
  @transient private val reader = new ConfigReader(settings)

  private def register(entry: ConfigEntry[_]): Unit = {
    require(!v2SqlConfEntries.containsKey(entry.key),
      s"Duplicate V2SQLConfigEntry. ${entry.key} has been registered")
    v2SqlConfEntries.put(entry.key, entry)
  }

  def buildConf(key: String): ConfigBuilder = ConfigBuilder(key).onCreate(register)

  def folk(update: Map[String, String]): V2SqlConf = {
    val result = new V2SqlConf(v2SqlConfEntries)
    getAllCustomConf ++ update foreach {
      case(k, v) if v != null => result.setConfString(k, v)
    }
    result
  }

  def getConf[T](entry: ConfigEntry[T], checkExist: Boolean = false): T = {
    require(v2SqlConfEntries.get(entry.key) == entry, s"$entry is not registered")
    val ret = entry.readFrom(reader)
    if (ret == null && checkExist) {
      throw new NoSuchElementException(s"value for key ${entry.key} not found")
    }
    ret
  }

  def getConf[T](entry: ConfigEntry[T], defaultValue: T): T = {
    require(v2SqlConfEntries.get(entry.key) == entry, s"$entry is not registered")
    Option(entry.readFrom(reader)).getOrElse(defaultValue)
  }

  def getAllConf: immutable.Map[String, String] = {
    v2SqlConfEntries.values().asScala
      .filter(entry => entry.defaultValue.isDefined)
      .map(entry => (entry.key, entry.defaultValueString))
      .toMap[String, String] ++
    getAllCustomConf
  }

  def getAllCustomConf: immutable.Map[String, String] = {
    settings.asScala.toMap
  }

  def setConf[T](entry: ConfigEntry[T], value: T): Unit = {
    require(!readOnly)
    require(entry != null, "entry cannot be null")
    require(value != null, s"value cannot be null for key: ${entry.key}")
    require(v2SqlConfEntries.get(entry.key) == entry, s"$entry is not registered")
    settings.put(entry.key, entry.stringConverter(value))
  }

  def setConfString(key: String, value: String): Unit = {
    require(!readOnly)
    require(key != null, "key cannot be null")
    require(value != null, s"value cannot be null for key: $key")
    val entry = v2SqlConfEntries.get(key)
    if (entry != null) {
      entry.valueConverter(value)
    }
    settings.put(key, value)
  }
}

object V2SqlConf {
  private val dsv2ConfEntryTypes = new jConcurrentHashMap[String, V2SqlConf]()

  def apply(v2Type: String): V2SqlConf = {
    var conf = dsv2ConfEntryTypes.get(v2Type)
    if (conf == null) {
      conf = new V2SqlConf()
      dsv2ConfEntryTypes.put(v2Type, conf)
    }
    conf
  }
}