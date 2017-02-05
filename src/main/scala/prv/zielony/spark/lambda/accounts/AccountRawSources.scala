package prv.zielony.spark.lambda.accounts

import org.apache.spark.sql.DataFrame

case class AccountRawSources(accounts: DataFrame, transactions: DataFrame)
