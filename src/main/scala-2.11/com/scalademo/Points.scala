package com.scalademo

import dbis.dbscan.{ClusterPoint, DBScan, GridPartitioner, Histogram}
import org.apache.log4j.Logger
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

case class NHConfig(input: java.net.URI = new java.net.URI("."),
                    output: java.net.URI = new java.net.URI("."),
                    numDimensions: Int = -1,
                    ppd: Int = 5,
                    samples: Int = 1000,
                    epsilon: Double = 0.5,
                    buckets: Int = 20)

object Points {

    var distanceFun: (Vector, Vector) => Double = null

    /**
      *
      * @param p
      * @param pts
      * @param epsilon
      * @return
      */
    def countNeighbors(p: ClusterPoint, pts: Iterable[ClusterPoint], epsilon: Double): Double = {
        pts.count{ cp => distanceFun(p.vec, cp.vec) <= epsilon } - 1.0
    }


    /**
      *
      * @param iter
      * @param maxPartitionSize
      * @param epsilon
      * @param nBuckets
      * @param nSamples
      * @return
      */
    def computeNeighborhoodHistogram(iter: Iterator[(Int, Iterable[(Int, ClusterPoint)])],
                                     maxPartitionSize: Long, epsilon: Double, nBuckets: Int,
                                     nSamples: Int): Iterator[Histogram] = {
        // construct an array of buckets
        val bucketWidth = maxPartitionSize / nBuckets.toDouble
        val histo = Histogram(nBuckets, bucketWidth)
        while (iter.hasNext) {
            val (_, objIter) = iter.next()
            val points = objIter.map { case (_, p) => p }.take(nSamples)
            // find minimal distances for all points
            if (points.size > 1) {
                val neighbors = points.map { p => countNeighbors(p, points, epsilon) }

                // update the histograms
                histo.updateBuckets(neighbors)
            }
        }
        val res = List(histo)
        res.iterator
    }

    def main(args: Array[String]) {
        val inputFile: String = "/usr/local/fproject/res/data/20160720.export.CSV"
        var outputFile: String = "/usr/local/fproject/res/points"
        var numDimensions: Int = 2
        var partitionsPerDimension: Int = 5
        var numBuckets: Int = 500
        var numSamples: Int = 1000
        var epsilon: Double = 0.470635222308477

        val log = Logger.getLogger(getClass.getName)

        val conf = new SparkConf().setAppName("DBSCAN: NeighborhoodHistogram").setMaster("local[4]")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
//         sc.setLogLevel("INFO")

        // though, we don't run DBSCAN we need an instance for getting access
        // to the partitioning and distance functions
        val dbscan = new DBScan()
        distanceFun = dbscan.distanceFun

        // load the data
        val df = Hi.getCoords(sc, sqlContext, inputFile)

        val data = df
            .map(r => Array(r.getDouble(0), r.getDouble(1)))
            .map(t=>Vectors.dense(t))

        // determine the MBB of the whole dataset
        val globalMBB = dbscan.getGlobalMBB(data)
        log.info(s"step 0: determining global MBB: $globalMBB")

        // we use a simple grid based partitioning without overlap here
        log.info("step 1: calculating the partitioning using the grid partitioner")
        val partitioner = new GridPartitioner().setMBB(globalMBB).setPPD(partitionsPerDimension)
        val partitionMBBs = partitioner.computePartitioning()

        // now we partition the input data according the partition MBBs
        log.info("step 2: partitioning the input")
        val mappedPoints = dbscan.partitionInput(data.map(p => ClusterPoint(p)), partitionMBBs)
        mappedPoints.cache()

        // the maximum number of points in the eps-neighborhood is estimated by
        // the number of points in the largest partition
        val maxPartitionSize = mappedPoints.countByKey().map{case (_,n) => n}.max

        val clusterSets = mappedPoints.groupBy(k => k._1)
        // and compute the histograms of minimal distances of points within their partitions
        val histograms = clusterSets.mapPartitions(iter =>
            computeNeighborhoodHistogram(iter, maxPartitionSize, epsilon, numBuckets, numSamples), true)

        // finally, we combine the bucket frequencies
        log.info("step 3: aggregate frequencies from all buckets")
        val finalHistogram = histograms.reduce{ case (hist1, hist2) => hist1.mergeBuckets(hist2) }

        // ... and save the result to the output file
        sc.parallelize(finalHistogram.buckets, 1).saveAsTextFile(outputFile.toString)
        sc.stop()
    }

}
