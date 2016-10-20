package com.scalademo

import org.apache.log4j.Logger
import dbis.dbscan._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

case class DHConfig(input: java.net.URI = new java.net.URI("."),
                    output: java.net.URI = new java.net.URI("."),
                    numDimensions: Int = -1,
                    ppd: Int = 5,
                    samples: Int = 1000,
                    buckets: Int = 20)

object Distance {
    var distanceFun: (Vector, Vector) => Double = null

    /**
      * Find the minimal distance from a point p to a list of points by ignoring the
      * point at position ignore.
      *
      * @param p the starting point
      * @param pts the list of points to which the distance is computed
      * @param ignore the index of point to be ignored
      * @return the minimal distance
      */
    def findMinDistance(p: ClusterPoint, pts: Iterable[(ClusterPoint, Int)], ignore: Int): Double = {
        pts.filter(_._2 != ignore).map{ case (cp, _) => distanceFun(p.vec, cp.vec) }.min
    }

    /**
      *
      * @param iter
      * @param maxDist
      * @param nBuckets
      * @param nSamples
      * @return
      */
    def computeDistanceHistogram(iter: Iterator[(Int, Iterable[(Int, ClusterPoint)])],
                                 maxDist: Double, nBuckets: Int, nSamples: Int): Iterator[Histogram] = {
        // construct an array of buckets
        val bucketWidth = maxDist / nBuckets.toDouble
        val histo = Histogram(nBuckets, bucketWidth)

        while (iter.hasNext) {
            val (_, objIter) = iter.next()
            val points = objIter.map { case (_, p) => p }.zipWithIndex.take(nSamples)
            // find minimal distances for all points
            if (points.size > 1) {
                val dists = points.map { case (p, i) => findMinDistance(p, points, i) }

                // update the histograms
                histo.updateBuckets(dists)
            }
        }
        val res = List(histo)
        res.iterator
    }

    /**
      * Calculates a vector of cell sizes (widths) for each dimension.
      *
      * @param globalMBB the MBB of the whole dataset
      * @param ppd number of partitions per dimension
      * @return a vector representing the size of a cell in each dimension
      */
    def calcPartitionVector(globalMBB: MBB, ppd: Int): Vector = {
        val max = globalMBB.maxVec.toArray
        val min = globalMBB.minVec.toArray
        val res = new Array[Double](max.length)
        for (i <- res.indices) {
            res(i) = (max(i) - min(i)) / ppd
        }
        Vectors.dense(res)
    }

    def main(args: Array[String]) {
        val inputFile: String = "/usr/local/fproject/res/data/20160720.export.CSV"
        var outputFile: String = "/usr/local/fproject/res/dists"
        var numDimensions: Int = 2
        var partitionsPerDimension: Int = 5
        var numBuckets: Int = 500
        var numSamples: Int = 1000
        var takeSample: Boolean = true

        val log = Logger.getLogger(getClass.getName)

        println("NDIMS = " + numDimensions)
        val conf = new SparkConf().setAppName("DBSCAN: DistanceHistogram").setMaster("local")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        // though, we don't run DBSCAN we need an instance for getting access
        // to the partitioning and distance functions
        val dbscan = new DBScan()
        distanceFun = dbscan.distanceFun

        // load the data
//        val data = sc.textFile(inputFile.toString())
//            .map(line => line.split(",").slice(0, numDimensions).map(_.toDouble))
//            .filter(arr => arr(0))
//            .map(t => Vectors.dense(t))
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

        // the maximum possible distance is determined by the size of a grid cell
        val partitionVec = calcPartitionVector(globalMBB, partitionsPerDimension)
        val maxDistance = distanceFun(partitionVec, Vectors.zeros(partitionVec.size))

        // now we partition the input data according the partition MBBs
        log.info("step 2: partitioning the input")
        val mappedPoints = dbscan.partitionInput(data.map(p => ClusterPoint(p)), partitionMBBs)
        val clusterSets = mappedPoints.groupBy(k => k._1)

        // and compute the histograms of minimal distances of points within their partitions
        val histograms = clusterSets.mapPartitions(iter =>
            computeDistanceHistogram(iter, maxDistance, numBuckets, numSamples), true)

        // finally, we combine the bucket frequencies
        log.info("step 3: aggregate frequencies from all buckets")
        val finalHistogram = histograms.reduce{ case (hist1, hist2) => hist1.mergeBuckets(hist2) }

        // ... and save the result to the output file
        sc.parallelize(finalHistogram.buckets, 1).saveAsTextFile(outputFile.toString)
        sc.stop()
    }
}