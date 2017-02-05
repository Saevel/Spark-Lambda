package prv.zielony.spark.lambda.accounts

import java.time.LocalDateTime

import cats.data.Reader
import org.apache.spark.sql._
import prv.zielony.spark.lambda._
import prv.zielony.spark.lambda.transactions.{TransactionEvent, TransactionEventType}

trait AccountFactory {

  def calculateAccountsIncrementally(implicit sparkSession: SparkSession): Reader[AccountRawSources, Dataset[Account]] =
    filterRawSources andThen calculateChanges andThen mergeChanges

  def calculateAccounts(implicit sparkSession: SparkSession): Reader[Dataset[TransactionEvent], Dataset[Account]] =
    calculateDeltas andThen transformToAccounts

  def filterRawSources(implicit sparkSession: SparkSession): Reader[AccountRawSources, AccountSources] = Reader { rawSources =>
    import sparkSession.implicits._
    val latestTimestamp = rawSources.accounts.select($"dt".as[Long]).reduce((first, second) =>
      if(first >= second) first else second
    )

    //Get only the latest version of accounts and trasanctions executed since they were calculated
    AccountSources(
      rawSources.accounts.filter($"dt".as[Long] >= latestTimestamp).drop($"dt").as[Account],
      rawSources.transactions.filter($"dt".as[Long] >= latestTimestamp).drop($"dt").as[TransactionEvent]
    )
  }

  private def calculateChanges(implicit sparkSession: SparkSession): Reader[AccountSources, AccountChanges] = Reader{ sources =>
    AccountChanges(sources.accounts, calculateDeltas(sparkSession)(sources.newEvents))
  }

  private def mergeChanges(implicit sparkSession: SparkSession): Reader[AccountChanges, Dataset[Account]] = Reader { changes =>
    import sparkSession.implicits._
    changes.accounts.joinWith(changes.changes, changes.accounts("id") === changes.changes("id"), "outer").map{
      case (account, null) => account
      case (null, delta) => Account(delta.id, delta.change)
      case (account, delta) => account.copy(balance = (account.balance + delta.change))
    }
  }

  private def calculateDeltas(implicit sparkSession: SparkSession): Reader[Dataset[TransactionEvent], Dataset[Delta]] = Reader { events =>
    import sparkSession.implicits._
    events.flatMap {
      case TransactionEvent(_, targetAccountId, amount, "Insertion") =>
        Traversable(Delta(targetAccountId, amount))
      case TransactionEvent(sourceAccountId, _, amount, "Withdrawal") =>
        Traversable(Delta(sourceAccountId, (-1) * amount))
      case TransactionEvent(sourceAccountId, targetAccountId, amount, "Transfer") =>
        Traversable(Delta(sourceAccountId, (-1 * amount)), Delta(targetAccountId, amount))
    }
      .groupByKey(_.id)
      .mapGroups{ case(id, deltas) => Delta(id, deltas.map(_.change).reduce(_+_))}
  }

  implicit class TimestampFilteredRecords(data: DataFrame) {
    def withTimestampAfter(timestamp: LocalDateTime)(implicit session: SparkSession): DataFrame = {
      import session.implicits._
      data.filter($"dt".as[Long] >= timestamp.toLong)
    }
  }

  private def transformToAccounts(implicit sparkSession: SparkSession): Reader[Dataset[Delta], Dataset[Account]] = Reader{ deltas =>
    import sparkSession.implicits._
    deltas.groupByKey(_.id)
      .mapGroups{ case (id, changes) => Account(id, changes.map(_.change).reduce(_+_))}
  }
}