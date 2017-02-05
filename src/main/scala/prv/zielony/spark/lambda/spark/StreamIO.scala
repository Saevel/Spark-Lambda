package prv.zielony.spark.lambda.spark

import cats.data.Reader
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

/**
  * Created by Zielony on 2017-02-04.
  */
trait StreamIO {

  def readJsonStream[T : Encoder](path: String): Reader[SparkSession, Dataset[T]] = Reader { session =>
    session.readStream.json(path).as[T]
  }

  def writeJsonStream[T](path: String, outputMode: OutputMode): Reader[Dataset[T], StreamingQuery] = Reader { dataset =>
    dataset.writeStream
      .outputMode(outputMode)
      .option("checkpointLocation", "lambda/checkpoints")
      .format("json")
      .start(path)
  }
}
