package prv.zielony.spark.lambda

import java.time.LocalDateTime

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import prv.zielony.spark.lambda.transactions.{TransactionEvent, TransactionEventType}
import prv.zielony.spark.lambda.accounts.{Account, AccountFactory}
import prv.zielony.spark.lambda.spark.{Spark, StreamIO}
import prv.zielony.spark.lambda._

object StreamingApplication extends App with Spark with AccountFactory with StreamIO {

  startSession
    .map{ session =>
      import session.implicits._
      session.readStream.schema(TransactionSchema).json("lambda/input/transactions").as[TransactionEvent]
    }
    .andThen(writeJsonStream[TransactionEvent](
      s"lambda/speed/transactions/timestamp=${LocalDateTime.now().dateTimeString}",
      OutputMode.Append())
    )
    .run(new SparkConf().setAppName("StreamingApplication").setMaster("local[*]"))
    .awaitTermination()

  object TransactionSchema extends StructType(Array(
    StructField("sourceAccountId", LongType),
    StructField("targetAccountId", LongType),
    StructField("amount", DoubleType),
    StructField("kind", StringType)
  ))
}
