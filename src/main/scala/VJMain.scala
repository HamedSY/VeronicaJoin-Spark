import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.log4j.{Level, Logger}
import VeronicaJoin.run
import org.apache.spark.partial.BoundedDouble
import org.apache.spark.partial.PartialResult
import scala.collection.mutable

object VJMain extends App {
  Logger.getRootLogger.setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org.spark_project").setLevel(Level.OFF)

  val inputFileR = "data/orkut-500k.txt"
  val inputFileS = "data/orkut-500k.txt"
  val threshold = 0.8

  val selfJoin = inputFileR == inputFileS

  val conf = new SparkConf().setAppName("VeronicaJoin").setMaster("local[*]")
    .set("spark.driver.memory", "16g") // Increase driver memory (e.g., 8GB)
    .set("spark.executor.memory", "16g") // Increase executor memory (e.g., 8GB)
    .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:G1ReservePercent=20")
    .set("spark.memory.fraction", "0.8")  // 80% for execution/storage, 20% for overhead
    .set("spark.memory.storageFraction", "0.4")  // Balance storage vs. execution
    .set("spark.ui.showConsoleProgress", "false")
    .set("spark.eventLog.enabled", "false")
  val sc = new SparkContext(conf)

  // Read datasets
  val datasetR = readDataset(sc, inputFileR)
  val datasetS = if (selfJoin) datasetR else readDataset(sc, inputFileS)

  val numRApprox: PartialResult[BoundedDouble] = datasetR.countApprox(1000)
  val numR = numRApprox.getFinalValue.mean.toLong
  val numS = if (selfJoin) numR else {
    val numSApprox: PartialResult[BoundedDouble] = datasetS.countApprox(1000)
    numSApprox.getFinalValue.mean.toLong
  }

  // Compute global token ordering
  val allTokens = if (selfJoin) datasetR.flatMap(_._2) else datasetR.flatMap(_._2).union(datasetS.flatMap(_._2))
  val totalOccurrencesApprox: PartialResult[BoundedDouble] = allTokens.countApprox(1000)
  val totalOccurrences = totalOccurrencesApprox.getFinalValue.mean.toLong
  val tokenFreq = allTokens.map((_, 1L)).reduceByKey(_ + _)
  val tokenFreqList = tokenFreq.sortBy(_._2).collect()
  val globalOrdering = tokenFreqList.map(_._1)
  val tokenRankMap = globalOrdering.zipWithIndex.toMap
  val tokenRank: Broadcast[Map[Int, Int]] = sc.broadcast(tokenRankMap)

  // Precompute ranked datasets
  val rankedR = datasetR.map { case (id, tokens) =>
    (id, tokens.flatMap(t => tokenRank.value.get(t).toSeq).sorted.toArray)
  }.cache()
  val rankedS = if (selfJoin) rankedR else datasetS.map { case (id, tokens) =>
    (id, tokens.flatMap(t => tokenRank.value.get(t).toSeq).sorted.toArray)
  }.cache()

  // Compute numPartitions
  val numSetsForAvg = if (selfJoin) numR else numR + numS
  val avgSize = if (numSetsForAvg == 0) 0.0 else totalOccurrences.toDouble / numSetsForAvg
  val avgPlen = if (avgSize == 0) 0.0 else avgSize - math.ceil(threshold * avgSize) + 1
  val expectedReplicated = ( (if (selfJoin) numR else numR + numS) * avgPlen ).toLong
  val targetPerPartition = 10000L
  val numPartitions = math.max(sc.defaultParallelism, if (expectedReplicated == 0 || targetPerPartition == 0) 1 else ((expectedReplicated + targetPerPartition - 1L) / targetPerPartition).toInt)

  // Balance partition assignment for ranks
  val tokenFreqMap = tokenFreqList.toMap
  val sortedTokensDesc = tokenFreqList.sortBy(-_._2).map(_._1)
  val partSums = Array.fill(numPartitions)(0L)
  val pq = mutable.PriorityQueue[(Long, Int)]()(Ordering.by[(Long, Int), Long](_._1)) // min-heap by sum
  for (p <- 0 until numPartitions) pq.enqueue((0L, p))
  val rankToPartArr = new Array[Int](globalOrdering.length)
  for (token <- sortedTokensDesc) {
    val freq = tokenFreqMap(token)
    val (s, p) = pq.dequeue()
    val rank = tokenRankMap(token)
    rankToPartArr(rank) = p
    pq.enqueue((s + freq, p))
  }
  val rankToPart: Broadcast[Array[Int]] = sc.broadcast(rankToPartArr)

  // Run similarity join with timing
  var sumRuntime: Double = 0
  val runNumber = 10
  for (i <- 0 until runNumber) {
    val startTime = System.nanoTime()
    val result = VeronicaJoin.run(sc, rankedR, rankedS, threshold, selfJoin, tokenRank, numR, numS, totalOccurrences, numPartitions, rankToPart)
    val count = result.count()
    val endTime = System.nanoTime()
    val runtime = (endTime - startTime) / 1e9 
    sumRuntime += runtime

    println(s"Output count: $count, Runtime: $runtime seconds")
  }

  val avgRuntime = sumRuntime / runNumber
  println(s"Average Runtime: $avgRuntime")

  rankedR.unpersist()
  if (!selfJoin) rankedS.unpersist()

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