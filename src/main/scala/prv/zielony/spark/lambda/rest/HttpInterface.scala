package prv.zielony.spark.lambda.rest

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.http.scaladsl._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import akka.stream.Materializer
import akka.util.Timeout
import cats.data.Reader
import prv.zielony.spark.lambda.accounts.AverageBalance

import scala.concurrent.{ExecutionContext, Future}


/**
  * Created by Zielony on 2017-02-04.
  */
trait HttpInterface {

  /*
   * TODO: Get a REST message, ping an actor. In the actor: Use a Spark context to batch-read: the batch views
   * and the newest part of the streaming (also as batch) => merge together & return result => return in REST
   */

  def startHttpServer(port: Int)(implicit materializer: Materializer,
                      actorSystem: ActorSystem,
                      executionContext: ExecutionContext,
                      defaultTimeout: Timeout): Reader[ActorRef, Future[Http.ServerBinding]] = Reader { accountsActor =>
    Http().bindAndHandle(
      path("accounts" / "average_balance") {
        get {
          onSuccess((accountsActor ? AverageBalance).map(_.toString))( average => complete(average))
        }
      //TODO: From config!
      } ~ path(""){complete("Hello, Http Application!")},
      "localhost",
      port
    )
  }
}
