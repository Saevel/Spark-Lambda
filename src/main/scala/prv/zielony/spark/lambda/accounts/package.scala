package prv.zielony.spark.lambda

import org.apache.spark.sql.Dataset

/**
  * Created by Zielony on 2017-02-04.
  */
package object accounts {

  case object AverageBalance

  case class AccountChanges(accounts: Dataset[Account], changes: Dataset[Delta])

  case class Delta(id: Long, change: Double)
}
