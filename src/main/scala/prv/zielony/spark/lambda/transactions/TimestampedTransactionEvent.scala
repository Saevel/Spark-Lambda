package prv.zielony.spark.lambda.transactions



/**
  * Created by Zielony on 2017-02-03.
  */
case class TimestampedTransactionEvent(sourceAccountId: Long,
                                       targetAccountId: Long,
                                       amount: Double,
                                       kind: String)
