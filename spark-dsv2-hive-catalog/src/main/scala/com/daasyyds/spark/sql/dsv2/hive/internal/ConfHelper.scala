package com.daasyyds.spark.sql.dsv2.hive.internal

import java.io.{File, FileNotFoundException}
import java.util.Locale

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.conf.V2SqlConf

object ConfHelper {
  val CATALOG_TYPE = "hive"
  val DEFAULT_HIVE_SITE_NAME = "hive-site.xml"
  val conf: V2SqlConf = V2SqlConf(CATALOG_TYPE)
  val HIVE_SITE_FILE = conf.buildConf("hive.conf")
    .doc("how to search hive-site.xml when a hive v2 catalog initializing, a path for classpath/local can be used.")
    .stringConf
    .transform(_.toLowerCase(Locale.ROOT))
    .checkValues(Set("classpath", "path"))
    .createWithDefault("path")
  val HIVE_SITE_FILE_CLASSPATH = conf.buildConf("hive.conf.classpath")
    .doc("a classpath will be used to initialize the hive v2 catalog, ex: v2/hive-site.xml")
    .stringConf
    .createOptional
  val HIVE_SITE_FILE_PATH = conf.buildConf("hive.conf.path")
    .doc("a local path will be used to initialize the hive v2 catalog, ex: /etc/hive/conf.v2/hive-site.xml")
    .stringConf
    .createOptional

  def checkAndGetHiveFilePath(path: String): String = {
    var f = new File(path)
    if (f.isDirectory) {
      f = new File(path, DEFAULT_HIVE_SITE_NAME)
      if (!f.exists()) {
        throw new FileNotFoundException(s"$path or ${f.getAbsolutePath}")
      }
    }
    f.getAbsolutePath
  }

  def checkAndGetHiveFileClasspath(path: String): String = {
    var res = Thread.currentThread().getContextClassLoader.getResource(path)
    if (res == null) {
      val p = new Path(path, DEFAULT_HIVE_SITE_NAME).toString
      res = Thread.currentThread().getContextClassLoader.getResource(p)
      if (res == null) {
        throw new FileNotFoundException(s"$path or $p")
      }
    }
    res.getPath
  }
}
