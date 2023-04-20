package sensorstats

import akka.actor.ActorSystem
import akka.stream.scaladsl.Keep
import akka.stream.testkit.javadsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import sensorstats.SensorStatistics.{calculateProcessingCounts, calculateStatisticFlow, filesCounter, sensorDataAggregation, source}

import java.nio.file.{Path, Paths}

class SensorStatisticsSpec extends AnyFlatSpec with BeforeAndAfterAll {
  implicit val actorSystem: ActorSystem = ActorSystem()
  val testDataDir: Path = Paths.get("src/test/resources/test-data")

  it should "emit files present in the given directory" in {
    val probe = source(testDataDir).toMat(TestSink.create(actorSystem))(Keep.right).run()
    probe.request(2)
    probe.expectNext(Paths.get("src/test/resources/test-data/leader-1.csv"))
  }

  it should "read files in the directory and emit measurements" in {
    val measurement1 = Measurement("s1", Some(10))
    val measurement2 = Measurement("s2", Some(88))

    val probe = source(testDataDir).via(sensorDataAggregation)
      .toMat(TestSink.create(actorSystem))(Keep.right).run()
    probe.request(2)
    probe.expectNext(measurement1)
    probe.expectNext(measurement2)
  }

  it should "aggregate sensor statistics" in {
    val sensorStats1 = SensorStats("s1", Some(10), Some(38.5), Some(67), 2)
    val sensorStats2 = SensorStats("s2", Some(60), Some(74), Some(88), 2)

    val probe = source(testDataDir).via(sensorDataAggregation).via(calculateStatisticFlow)
      .toMat(TestSink.create(actorSystem))(Keep.right).run()
    probe.request(2)
    probe.expectNext(sensorStats1)
    probe.expectNext(sensorStats2)
  }

  it should "return number of files processed" in {
    val (sourceProbe, sinkProbe) = TestSource[Path].via(filesCounter).toMat(TestSink.create(actorSystem))(Keep.both).run()
    sourceProbe.sendNext(Paths.get("src/test/resources/test-data/leader-1.csv"))
    sourceProbe.sendNext(Paths.get("src/test/resources/test-data/leader-2.csv"))
    sourceProbe.sendComplete()

    sinkProbe.request(1)
    sinkProbe.expectNext(2)
  }

  it should "calculate total measurement processed and failed measurement count" in {
    val measurement1 = Measurement("s1", Some(10))
    val measurement2 = Measurement("s2", Some(30))
    val measurement3 = Measurement("s1", None)
    val measurement4 = Measurement("s2", Some(34))
    val measurement5 = Measurement("s1", Some(80))
    val measurement6 = Measurement("s2", None)

    val (sourceProbe, sinkProbe) = TestSource[Measurement]()
      .via(calculateProcessingCounts)
      .toMat(TestSink.create(actorSystem))(Keep.both).run()

    sourceProbe.sendNext(measurement1)
    sourceProbe.sendNext(measurement2)
    sourceProbe.sendNext(measurement3)
    sourceProbe.sendNext(measurement4)
    sourceProbe.sendNext(measurement5)
    sourceProbe.sendNext(measurement6)
    sourceProbe.sendComplete()

    sinkProbe.request(1)
    sinkProbe.expectNext((6, 2))
  }

  override def afterAll(): Unit = actorSystem.terminate()
}
