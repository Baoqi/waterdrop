package io.github.interestinglab.waterdrop.input.batch

import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Sql extends BaseStaticInput {

  var config: Config = ConfigFactory.empty()
  var sqlFilter = new io.github.interestinglab.waterdrop.filter.Sql()

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {
    this.config = config
    sqlFilter.setConfig(config)
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = sqlFilter.checkConfig()

  /**
   * */
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    sqlFilter.process(spark, null)
  }
}
