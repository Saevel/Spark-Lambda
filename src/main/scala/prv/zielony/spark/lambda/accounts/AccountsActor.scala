package prv.zielony.spark.lambda.accounts

import java.time.LocalDateTime

import akka.actor.Actor
import akka.actor.Actor.Receive
import cats.data.Reader
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._
import prv.zielony.spark.lambda._
import prv.zielony.spark.lambda.spark.Spark
import prv.zielony.spark.lambda.transactions.TransactionEvent
import org.apache.spark.sql.expressions.scalalang.typed._

/**
  * Created by Zielony on 2017-02-04.
  */
class AccountsActor(sparkSession: SparkSession) extends Actor with AccountFactory{

  implicit val session: SparkSession = sparkSession

  import sparkSession.implicits._

  override def receive: Receive = {
    case AverageBalance => {
      sender ! calculateAccountsIncrementally
        .andThen(calculateBalanceAverage)
        .run(
          AccountRawSources (
            sparkSession.read.schema(AccountSchema).json("lambda/batch/accounts"),
            sparkSession.read.schema(TransactionSchema).json("lambda/speed/transactions")
          )
        )
    }
  }

  private def calculateBalanceAverage: Reader[Dataset[Account], Double] = Reader { accounts =>
    val (size, sum) = accounts.map(account => (1, account.balance)).reduce { (first, second) =>
      ((first._1 + second._1), (first._2 + second._2))
    }
    (sum / size)
  }

  //TODO: Inject!

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
