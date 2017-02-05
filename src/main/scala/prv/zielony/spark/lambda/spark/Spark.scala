package prv.zielony.spark.lambda.spark

import cats.data.Reader
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by Zielony on 2017-02-03.
  */
trait Spark {

  def startSession: Reader[SparkConf, SparkSession] = Reader( conf =>
    SparkSession.builder.config(conf).getOrCreate()
  )
}
