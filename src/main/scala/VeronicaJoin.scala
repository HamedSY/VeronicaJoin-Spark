import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkContext, HashPartitioner}
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.math

object VeronicaJoin {

  private class Bucket {
    val invertedList = new mutable.ArrayBuffer[(Int, Int)]() // store idx in collection and length of record
    var listPos = 0
  }

  private def fastJaccard(r: Array[Int], s: Array[Int], threshold: Double, pr0: Int = 0, ps0: Int = 0, sOverlap: Int = 0): Boolean = {
    var pr = pr0
    var ps = ps0
    var maxR = r.length - pr + sOverlap
    var maxS = s.length - ps + sOverlap

    var overlap = sOverlap
    val ceilValue = math.ceil(threshold * (r.length + s.length) / (1.0 + threshold)).toInt
    val eqOverlap = math.min(r.length, math.min(s.length, ceilValue))

    while (maxR >= eqOverlap && maxS >= eqOverlap && overlap < eqOverlap) {
      val d = r(pr) - s(ps)
      if (d == 0) {
        pr += 1
        ps += 1
        overlap += 1
      } else if (d < 0) {
        pr += 1
        maxR -= 1
      } else {
        ps += 1
        maxS -= 1
      }
    }

    overlap >= eqOverlap
  }

  private def verifyCandidates(indexed: Array[(Long, Array[Int])], probingId: Long, probingRecord: Array[Int], candidates: Array[Int], cPos: Int, overlaps: Array[Int], threshold: Double, probingPrefixLength: Int, selfJoin: Boolean): Array[(Long, Long)] = {
    val result = new mutable.ArrayBuffer[(Long, Long)]()
    var cIdx = 0

    while (cIdx < cPos) {
      val (indexedId, indexedRecord) = indexed(candidates(cIdx))
      val overlap = overlaps(candidates(cIdx))

      var indexedPrefixLength = 0

      if (selfJoin) {
        val seqOverlap = threshold * (indexedRecord.length + indexedRecord.length) / (1.0 + threshold)
        val seqOverlapCeil = math.min(indexedRecord.length, math.ceil(seqOverlap).toInt)
        indexedPrefixLength = indexedRecord.length - seqOverlapCeil + 1

      } else {
        val indexedLowerBound = math.ceil(indexedRecord.length.toDouble * threshold).toInt
        indexedPrefixLength = indexedRecord.length - indexedLowerBound + 1
      }

      val probingPrefixLast = probingRecord(probingPrefixLength - 1)
      val indexedPrefixLast = indexedRecord(indexedPrefixLength - 1)

      if (probingPrefixLast < indexedPrefixLast) {
        if (fastJaccard(probingRecord, indexedRecord, threshold, probingPrefixLength, overlap, overlap)) {
          result += ((math.min(probingId, indexedId), math.max(probingId, indexedId)))
        }
      } else {
        if (fastJaccard(probingRecord, indexedRecord, threshold, overlap, indexedPrefixLength, overlap)) {
          result += ((math.min(probingId, indexedId), math.max(probingId, indexedId)))
        }
      }

      overlaps(candidates(cIdx)) = 0
      cIdx += 1
    }

    result.toArray
  }

  private def localRun(collectionR: Array[(Long, Array[Int])], collectionS: Array[(Long, Array[Int])], threshold: Double, selfJoin: Boolean): Array[(Long, Long)] = {
    val result = new mutable.ArrayBuffer[(Long, Long)]()
    val index = new mutable.HashMap[Int, Bucket]()

    val overlaps = Array.fill(collectionR.length)(0)
    val candidates = new Array[Int](collectionR.length)
    var cPos = 0

    // order collections by length
    val sortedR = collectionR.sortBy { case (_, arr) => arr.length }
    val sortedS = collectionS.sortBy { case (_, arr) => arr.length }

    if (selfJoin) {
      for (idxR <- sortedR.indices) {
        val (idR, recordR) = sortedR(idxR)
        val rLowerBound = math.ceil(recordR.length.toDouble * threshold).toInt
        val rPrefixLength = recordR.length - rLowerBound + 1

        for (p <- 0 until rPrefixLength) {
          val token = recordR(p)
          index.get(token) match {
            case Some(bucket) => {
              for (i <- bucket.listPos until bucket.invertedList.length) {
                val (idxS, sLength) = bucket.invertedList(i)

                if (sLength < rLowerBound) {
                  bucket.listPos += 1

                } else {
                  if (overlaps(idxS) == 0) {
                    candidates(cPos) = idxS
                    cPos += 1
                  }
                  overlaps(idxS) += 1
                }
              }
            }
            case None => // do nothing
          }
        }

        // for self-joins and ordered collections, we can use a shorter prefix length for indexing
        val reqOverlap = threshold * (recordR.length + recordR.length).toDouble / (1.0 + threshold)
        val reqOverlapCeil = math.min(recordR.length, math.ceil(reqOverlap).toInt)
        val rIndexPrefixLength = recordR.length - reqOverlapCeil + 1

        for (p <- 0 until rIndexPrefixLength) {
          val token = recordR(p)
          val bucket = index.getOrElseUpdate(token, new Bucket)
          bucket.invertedList += ((idxR, recordR.length))
        }

        if (cPos > 0) {
          result ++= verifyCandidates(sortedR, idR, recordR, candidates, cPos, overlaps, threshold, rPrefixLength, selfJoin)
          cPos = 0
        }
      }

    } else { // R-S join
      // build index
      for (idxR <- sortedR.indices) {
        val (idR, recordR) = sortedR(idxR)
        val rLowerBound = math.ceil(recordR.length.toDouble * threshold).toInt
        val rPrefixLength = recordR.length - rLowerBound + 1

        for (p <- 0 until rPrefixLength) {
          val token = recordR(p)
          val bucket = index.getOrElseUpdate(token, new Bucket)
          bucket.invertedList += ((idxR, recordR.length))
        }
      }

      // probe
      for (idxS <- sortedS.indices) {
        val (idS, recordS) = sortedS(idxS)
        val sLowerBound = math.ceil(recordS.length.toDouble * threshold).toInt
        val sUpperBound = math.floor(recordS.length.toDouble / threshold).toInt
        val sPrefixLength = recordS.length - sLowerBound + 1

        for (p <- 0 until sPrefixLength) {
          val token = recordS(p)
          index.get(token) match {
            case Some(bucket) => {
              var i = bucket.listPos
              while (i < bucket.invertedList.length) {
                val (idxR, rLength) = bucket.invertedList(i)
                if (rLength < sLowerBound) {
                  bucket.listPos += 1

                } else if (rLength <= sUpperBound) {
                  if (overlaps(idxR) == 0) {
                    candidates(cPos) = idxR
                    cPos += 1
                  }
                  overlaps(idxR) += 1

                } else {
                  i = bucket.invertedList.length // break
                }

                i += 1
              }
            }
            case None => // do nothing
          }
        }

        if (cPos > 0) {
          result ++= verifyCandidates(sortedR, idS, recordS, candidates, cPos, overlaps, threshold, sPrefixLength, selfJoin)
          cPos = 0
        }
      }
    }

    result.toArray
  }

  def run(sc: SparkContext, collectionR: RDD[(Long, Array[Int])], collectionS: RDD[(Long, Array[Int])], threshold: Double, selfJoin: Boolean, tokenRank: Broadcast[Map[Int, Int]], numR: Long, numS: Long, totalOccurrences: Long): RDD[(Long, Long)] = {
    def tokensToRanks(tokens: Array[Int]): Array[Int] = {
      tokens.flatMap(t => tokenRank.value.get(t).toSeq).sorted.toArray
    }

    def prefixLen(size: Int): Int = {
      if (size == 0) 0 else size - math.ceil(threshold * size).toInt + 1
    }

    // Compute number of partitions adaptively
    val numSetsForAvg = if (selfJoin) numR else numR + numS
    val avgSize = if (numSetsForAvg == 0) 0.0 else totalOccurrences.toDouble / numSetsForAvg
    val avgPlen = if (avgSize == 0) 0.0 else avgSize - math.ceil(threshold * avgSize) + 1
    val expectedReplicated = ( (if (selfJoin) numR else numR + numS) * avgPlen ).toLong
    val targetPerPartition = 2000L  // Further reduced for smaller partitions to mitigate OOM
    val numPartitions = math.max(sc.defaultParallelism, if (expectedReplicated == 0 || targetPerPartition == 0) 1 else ((expectedReplicated + targetPerPartition - 1L) / targetPerPartition).toInt)

    // Stage 2: RID-Pair Generation with optimized partitioning
    val rPrefixRDD: RDD[(Int, (String, Long, Array[Int]))] = collectionR.flatMap { case (id, tokens) =>
      val ranks = tokensToRanks(tokens)
      val plen = prefixLen(ranks.length)
      if (plen <= 0) {
        Seq.empty
      } else {
        val prefix = ranks.take(plen)
        val groups = prefix.map(r => r % numPartitions).toSet
        groups.map(g => (g, ("R", id, ranks)))
      }
    }

    val sPrefixRDD: RDD[(Int, (String, Long, Array[Int]))] = if (!selfJoin) {
      collectionS.flatMap { case (id, tokens) =>
        val ranks = tokensToRanks(tokens)
        val plen = prefixLen(ranks.length)
        if (plen <= 0) {
          Seq.empty
        } else {
          val prefix = ranks.take(plen)
          val groups = prefix.map(r => r % numPartitions).toSet
          groups.map(g => (g, ("S", id, ranks)))
        }
      }
    } else {
      sc.emptyRDD
    }

    // No cache to save memory
    // rPrefixRDD.cache()
    // if (!selfJoin) sPrefixRDD.cache()

    val dataRDD = rPrefixRDD.union(sPrefixRDD).partitionBy(new HashPartitioner(numPartitions))

    val groupedRDD = dataRDD.groupByKey()

    val candidatePairs = groupedRDD.flatMap { case (_, iter) =>
      val items = iter.toArray // Use array for faster local processing
      val rLocal = items.filter(_._1 == "R").map { case (_, id, tok) => (id, tok) }
      val sLocal = if (selfJoin) rLocal else items.filter(_._1 == "S").map { case (_, id, tok) => (id, tok) }
      localRun(rLocal.sortBy(_._2.length), sLocal.sortBy(_._2.length), threshold, selfJoin).toIterable
    }

    // No unpersist needed since no cache

    candidatePairs.distinct()
  }
}