package io.github.interestinglab.waterdrop.output.batch

import java.sql.{Connection, DriverManager}

import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConverters._

/**
 * Jdbc Sql Output is able to specify driver class while Mysql Output's driver is bound to com.mysql.jdbc.Driver etc.
 * When using Jdbc Output, class of jdbc driver must can be found in classpath.
 * JDBC Sql Output supports at least: MySQL, Oracle, PostgreSQL, SQLite
 * User can sepecify customized sql to insert data, so that we can use sql like:
 *   insert into test_t2 (col1, col2) values (?, ?) ON CONFLICT (col1) DO NOTHING
 *
 * NOTE: please make sure the count of "?" in insert sql matches the column count of previous dataset
 * */
class JdbcSql extends BaseOutput {

  var config: Config = ConfigFactory.empty()

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {

    // TODO: are user, password required ?
    val requiredOptions = List("driver", "url", "sql", "user", "password");

    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    if (nonExistsOptions.length == 0) {
      (true, "")
    } else {
      (
        false,
        "please specify " + nonExistsOptions
          .map { option =>
            val (name, exists) = option
            "[" + name + "]"
          }
          .mkString(", ") + " as non-empty string"
      )
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "batchsize" -> 1000
      ).asJava
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(df: Dataset[Row]): Unit = {

    val prop = new java.util.Properties
    prop.setProperty("driver", config.getString("driver"))
    prop.setProperty("user", config.getString("user"))
    prop.setProperty("password", config.getString("password"))


    df.foreachPartition((part: Iterator[Row]) => {
      var connection: Connection = null
      try {
        connection = DriverManager.getConnection(config.getString("url"), prop)
        val statement = connection.prepareStatement(config.getString("sql"))
        part.grouped(config.getInt("batchsize")).foreach{ batch =>
          batch.foreach { row => {
            Range(0, row.length).foreach{ i => statement.setObject(i + 1, row.get(i))}
            statement.addBatch()
          }}
          statement.executeBatch()
        }
      } finally {
        if (connection != null) {
          connection.close()
        }
      }
    })
  }
}
