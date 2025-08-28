import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.log4j.{Level, Logger}

object VJMain extends App {
  Logger.getRootLogger.setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org.spark_project").setLevel(Level.OFF)

  val inputFileR = "data/bms-pos-full.txt"
  val inputFileS = "data/bms-pos-full.txt"
  val threshold = 0.8

  val selfJoin = inputFileR == inputFileS

  val conf = new SparkConf().setAppName("VeronicaJoin").setMaster("local[*]")
    .set("spark.ui.showConsoleProgress", "false")
    .set("spark.eventLog.enabled", "false")
  val sc = new SparkContext(conf)

  // Read datasets
  val datasetR = readDataset(sc, inputFileR)
  val datasetS = if (selfJoin) datasetR else readDataset(sc, inputFileS)
  datasetR.cache()
  if (!selfJoin) datasetS.cache()

  // Compute global token ordering
  val allTokens = if (selfJoin) datasetR.flatMap(_._2) else datasetR.flatMap(_._2).union(datasetS.flatMap(_._2))
  val tokenFreq = allTokens.map((_, 1L)).reduceByKey(_ + _)
  val globalOrdering = tokenFreq.sortBy(_._2).keys.collect()
  val tokenRank: Broadcast[Map[Int, Int]] = sc.broadcast(globalOrdering.zipWithIndex.toMap)

  // Run similarity join with timing
  val startTime = System.nanoTime()
  val result = VeronicaJoin.run(sc, datasetR, datasetS, threshold, selfJoin, tokenRank)
  val count = result.count()
  val endTime = System.nanoTime()
  val runtime = (endTime - startTime) / 1e9 // Convert to seconds

  println(s"Output count: $count, Runtime: $runtime seconds")

  sc.stop()

  private def readDataset(sc: SparkContext, path: String): RDD[(Long, Array[Int])] = {
    sc.textFile(path)
      .zipWithIndex()
      .map { case (line, idx) =>
        val tokens = line.trim.split("\\s+").filter(_.nonEmpty).map(_.toInt).distinct.sorted
        (idx, tokens)
      }
  }
}