package prv.zielony.spark.lambda

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.util.Timeout
import cats.data.Reader
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import prv.zielony.spark.lambda.accounts.AccountsActor
import prv.zielony.spark.lambda.rest.HttpInterface
import prv.zielony.spark.lambda.spark.Spark

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * Created by Zielony on 2017-02-04.
  */
object HttpApplication extends App with Spark with HttpInterface {

  //TODO: wymyślić sposób żeby czytać tylko nowe streaming event'y !!!

  implicit val actorSystem: ActorSystem = ActorSystem("HttpApplication")

  implicit val materializer = ActorMaterializer()

  implicit val defaultTimeout: Timeout = 3 minutes

  (startSession andThen createAccountsActor andThen startHttpServer(9090)).run(
    new SparkConf().setAppName("HttpApplication").setMaster("local[*]")
  )

  private def createAccountsActor(implicit actorSystem: ActorSystem): Reader[SparkSession, ActorRef] = Reader(session =>
    actorSystem.actorOf(Props(new AccountsActor(session)))
  )
}