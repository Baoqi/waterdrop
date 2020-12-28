package io.github.interestinglab.waterdrop.input.batch

import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import io.github.interestinglab.waterdrop.config.TypesafeConfigUtils

import org.apache.spark.sql.{DataFrameReader, Dataset, Row, SparkSession}
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConversions._


class Jdbc extends BaseStaticInput {

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  private def checkTableOrQuery() = {
    val tableExists = config.hasPath("table")
    val queryExists = config.hasPath("query")

    if (!tableExists && !queryExists) {
      (
        false,
        "please specify either [table] or [query] as non-empty string")
    } else if (tableExists && queryExists) {
      (
        false,
        "you can only select one from either [table] or [query], but not both")
    } else {
      (true, "")
    }
  }

  override def checkConfig(): (Boolean, String) = {

    val requiredOptions = List("url", "user", "password", "driver");

    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    if (nonExistsOptions.isEmpty) {
      checkTableOrQuery()
    } else {
      (
        false,
        "please specify " + nonExistsOptions
          .map { case (field, _) => "[" + field + "]" }
          .mkString(", ") + " as non-empty string")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
  }

  def jdbcReader(sparkSession: SparkSession, driver: String): DataFrameReader = {

    var reader = sparkSession.read
      .format("jdbc")
      .option("url", config.getString("url"))
      .option("user", config.getString("user"))
      .option("password", config.getString("password"))
      .option("driver", driver)

    if (config.hasPath("table")) {
      reader = reader.option("dbtable", config.getString("table"))
    }
    if (config.hasPath("query")) {
      reader = reader.option("query", config.getString("query"))
    }

    Try(TypesafeConfigUtils.extractSubConfigThrowable(config, "jdbc.", false)) match {

      case Success(options) => {
        val optionMap = options
          .entrySet()
          .foldRight(Map[String, String]())((entry, m) => {
            m + (entry.getKey -> entry.getValue.unwrapped().toString)
          })

        reader.options(optionMap)
      }
      case Failure(exception) => // do nothing
    }

    reader
  }

  override def getDataset(spark: SparkSession): Dataset[Row] = {
    jdbcReader(spark, config.getString("driver")).load()
  }
}
