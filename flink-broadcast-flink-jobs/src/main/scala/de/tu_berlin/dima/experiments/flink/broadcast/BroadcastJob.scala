package de.tu_berlin.dima.experiments.flink.broadcast

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

import scala.collection.convert.wrapAsScala._

/**
  * Flink job that distributes of DataSet of longs with a given size (in mb) to a set amount of map task and does some
  * arbitrary computation.
  */
object BroadcastJob {

  val BYTES_PER_LONG = 8
  val BYTES_PER_MB = 1024 * 1024

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Not enough parameters")
      System.err.println("Usage: <jar> [number of map tasks] [size of broadcast in MB] [output path]")
      System.exit(-1)
    }

    val vectorSizeInMB = args(1).toLong
    val outputPath = args(2)

    val numMapTasks = vectorSizeInMB * BYTES_PER_MB / BYTES_PER_LONG

    val env = ExecutionEnvironment.getExecutionEnvironment

    // generate a NumberSequence to map over
    // one number per task/dop
    val matrix: DataSet[Long] = env
      .fromParallelCollection(new NumberSequenceIteratorWrapper(1, numMapTasks))
      .setParallelism(numMapTasks)
      .name(s"Generate mapper dataset [1..$numMapTasks]")

    val result: DataSet[Long] = matrix.map(new RichMapFunction[Long, Long] {
      override def map(value: Long): Long = {
        value
      }
    })

    result.writeAsText(outputPath, WriteMode.OVERWRITE)

    env.execute(s"dstat checker")
  }
}
