package prv.zielony.spark.lambda.transactions

/**
  * Created by Zielony on 2017-02-03.
  */
object TransactionEventType {

  case object Insertion extends TransactionEventType("Insertion")

  case object Withdrawal extends TransactionEventType("Withdrawal")

  case object Transfer extends TransactionEventType("Transfer")
}

class TransactionEventType(value: String)
