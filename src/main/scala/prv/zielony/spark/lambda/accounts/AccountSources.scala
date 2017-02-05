package prv.zielony.spark.lambda.accounts

import org.apache.spark.sql.Dataset
import prv.zielony.spark.lambda.transactions.TransactionEvent

/**
  * Created by Zielony on 2017-02-04.
  */
case class AccountSources(accounts: Dataset[Account], newEvents: Dataset[TransactionEvent]) {

}
