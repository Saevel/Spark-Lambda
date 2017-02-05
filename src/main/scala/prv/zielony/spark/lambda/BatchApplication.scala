package prv.zielony.spark.lambda

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

import cats.data.Reader
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}
import prv.zielony.spark.lambda.transactions.TransactionEvent
import prv.zielony.spark.lambda.accounts.{Account, AccountFactory, AccountRawSources, AccountSources}
import prv.zielony.spark.lambda.spark.Spark

object BatchApplication extends App with Spark with AccountFactory {

  val defaultFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-hh-mm-ss")

  implicit val sparkSession = SparkSession.builder().config(
    new SparkConf().setAppName("BatchApplication").setMaster("local[*]")
  ).getOrCreate()

  import sparkSession.implicits._

  calculateAccountsIncrementally.run(
    AccountRawSources(
      sparkSession.read.schema(AccountSchema).json("lambda/batch/accounts"),
      sparkSession.read.schema(TransactionSchema).json("lambda/speed/transactions")
    )
  ) .coalesce(1)
    .write
    .json(s"lambda/batch/accounts/dt=${LocalDateTime.now().dateTimeString}")

  private def timestampString: String = s"dt=${defaultFormatter.format(LocalDateTime.now())}"

  object AccountSchema extends StructType(Array(
    StructField("id", LongType),
    StructField("balance", DoubleType),
    StructField("dt", LongType)
  ))

  object TransactionSchema extends StructType(Array(
    StructField("sourceAccountId", LongType),
    StructField("targetAccountId", LongType),
    StructField("amount", DoubleType),
    StructField("kind", StringType),
    StructField("dt", LongType)
  ))
}