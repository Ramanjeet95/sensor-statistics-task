package sensorstats

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.ClosedShape
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, Framing, GraphDSL, RunnableGraph, Sink, Source, Zip}
import akka.util.ByteString

import java.nio.file.{Files, Path, Paths}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.sys.exit
import scala.util.Try

case class SensorStats(sensorId: String, min: Option[Int], avg: Option[Double], max: Option[Int], count: Int)
case class Measurement(sensorId: String, humidity: Option[Int])

object SensorStatistics {
  implicit val actorSystem: ActorSystem = ActorSystem()
  implicit val ec: ExecutionContextExecutor = actorSystem.dispatcher
  /**
   * reads files from specified directory
   * @param directory path to the directory
   * @return a Source for all csv files contained in the directory
   * */
  def source(directory: Path): Source[Path, NotUsed] =
    Directory.ls(directory).filter(Files.isRegularFile(_))
      .filter(_.getFileName.toString.matches(""".+\.csv"""))

  /**
   * reads data from files and prepares Measurement data flow
   * */
  def sensorDataAggregation: Flow[Path, Measurement, NotUsed] = Flow[Path]
    .flatMapConcat(FileIO.fromPath(_)
      .via(Framing.delimiter(ByteString("\n"), Int.MaxValue, allowTruncation = true))
      .map(_.utf8String).drop(1)
      .map(_.split(","))
      .map { case Array(sensorId, humidity) =>
        Measurement(sensorId, Try(humidity.trim.toInt).toOption)
      })

  /**
   * Calculates Minimum, Average, Maximum humidity measurement for each sensorId
   * */
  def calculateStatisticFlow: Flow[Measurement, SensorStats, NotUsed] =
    Flow[Measurement]
      .groupBy(1000, _.sensorId)
      .fold(SensorStats("", None, None, None, 0)) {
        (stat, data) =>
          val sId = data.sensorId
          val humidity = data.humidity

          val minimum = (stat.min, humidity) match {
            case (Some(m), Some(h)) => Some(m min h)
            case (None, Some(h)) => Some(h)
            case _ => stat.min
          }

          val maximum = (stat.max, humidity) match {
            case (Some(m), Some(h)) => Some(m max h)
            case (None, Some(h)) => Some(h)
            case _ => stat.max
          }

          val average: Option[Double] = (stat.avg, humidity) match {
            case (Some(a), Some(h)) => Some(((a * stat.count) + h) / (stat.count + 1))
            case (None, Some(h)) => Some(h)
            case _ => stat.avg
          }

          val count = if (humidity.isDefined) stat.count + 1 else stat.count
          SensorStats(sId, minimum, average, maximum, count)
      }.mergeSubstreams

  def metadataSink: Sink[(Int, (Int, Int)), Future[Done]] = Sink.foreach[(Int, (Int, Int))](x => {
    println(s"Number of processed files : ${x._1}")
    println(s"Number of processed measurements : ${x._2._1}")
    println(s"Number of failed measurements : ${x._2._2}")
    println()
  })

  /**
   * A Sink that sorts Sensor Statistics in descending order of average humidity and prints to the console
   * */
  def printStatsSink: Sink[SensorStats, Unit] = Sink.seq[SensorStats]
    .mapMaterializedValue(_.map(_.sortBy(_.avg)(Ordering.Option[Double].reverse)) map { sensorStats =>
      println("Sensors with highest avg humidity:")
      println()
      println("sensor-id,min,avg,max")
      sensorStats.foreach {
        stat => println(s"${stat.sensorId},${stat.min.fold("NaN")(min => s"$min")},${stat.avg.fold("NaN")(avg => s"$avg")},${stat.max.fold("NaN")(max => s"$max")}")
      }
    }).mapMaterializedValue(_.onComplete(_ => actorSystem.terminate()))

  /**
   * calculates number processed and failed measurements
   * */
  def calculateProcessingCounts: Flow[Measurement, (Int, Int), NotUsed] = Flow[Measurement].fold((0, 0)) {
    case ((processedCount, failedCount), curr) => if (curr.humidity.isEmpty) (processedCount + 1, failedCount + 1) else (processedCount + 1, failedCount)
  }

  /**
   * calculates number of files in the stream
   * */
  def filesCounter: Flow[Path, Int, NotUsed] = Flow[Path].fold(0)((acc, _) => acc + 1)

  def main(args: Array[String]): Unit = {
    if(args.isEmpty) {
      println("please enter a valid directory")
      exit(1)
    }

    val directory = Paths.get(args(0))

    //Winding everything together
    val graph = GraphDSL.create() {
      implicit builder =>
        import GraphDSL.Implicits._
        val sourceBroadcaster = builder.add(Broadcast[Path](2))
        val sensorDataBroadcaster = builder.add(Broadcast[Measurement](2))
        val zipCounts = builder.add(Zip[Int, (Int, Int)])

        source(directory) ~> sourceBroadcaster ~> filesCounter ~> zipCounts.in0
        sourceBroadcaster ~> sensorDataAggregation ~> sensorDataBroadcaster ~> calculateProcessingCounts ~> zipCounts.in1
        zipCounts.out ~> metadataSink
        sensorDataBroadcaster ~> calculateStatisticFlow ~>  printStatsSink
        ClosedShape
    }

    RunnableGraph.fromGraph(graph).run()
  }
}
