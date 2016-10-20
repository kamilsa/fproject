package com.scalademo

import java.text.DecimalFormat

import dbis.dbscan.DBScan
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.{StructType, _}


import scala.collection.mutable
import scala.reflect.io.Path
import scala.util.Try

/**
  * @author Farida Makhmutova on 07/07/16
  */
object Hi {

//    val dataFiles = "/usr/local/fproject/res/data/2016070{1,2,3,4,5,6,7}.export.CSV"
    val dataFiles = "/usr/local/fproject/res/data/20160720.export.CSV"
    val cols = "GLOBALEVENTID\tSQLDATE\tMonthYear\tYear\tFractionDate\tActor1Code\tActor1Name\tActor1CountryCode\tActor1KnownGroupCode\tActor1EthnicCode\tActor1Religion1Code\tActor1Religion2Code\tActor1Type1Code\tActor1Type2Code\tActor1Type3Code\tActor2Code\tActor2Name\tActor2CountryCode\tActor2KnownGroupCode\tActor2EthnicCode\tActor2Religion1Code\tActor2Religion2Code\tActor2Type1Code\tActor2Type2Code\tActor2Type3Code\tIsRootEvent\tEventCode\tEventBaseCode\tEventRootCode\tQuadClass\tGoldsteinScale\tNumMentions\tNumSources\tNumArticles\tAvgTone\tActor1Geo_Type\tActor1Geo_FullName\tActor1Geo_CountryCode\tActor1Geo_ADM1Code\tActor1Geo_Lat\tActor1Geo_Long\tActor1Geo_FeatureID\tActor2Geo_Type\tActor2Geo_FullName\tActor2Geo_CountryCode\tActor2Geo_ADM1Code\tActor2Geo_Lat\tActor2Geo_Long\tActor2Geo_FeatureID\tActionGeo_Type\tActionGeo_FullName\tActionGeo_CountryCode\tActionGeo_ADM1Code\tActionGeo_Lat\tActionGeo_Long\tActionGeo_FeatureID\tDATEADDED\tSOURCEURL"


    def initSc(): (SparkContext, SQLContext) = {
        val conf = new SparkConf()
            .setAppName("Simple Application")
                    .setMaster("local[*]")
        //            .setMaster("spark://10.91.36.100:7077")
        //            .setJars(Array("lib/dbscan-0.1.jar","lib/spatialpartitioner_2.11-0.1-SNAPSHOT.jar"))
//                    .set("spark.executor.memory", "6g")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        return (sc, sqlContext)
    }

    def getCoords(sc: SparkContext, sqlContext: SQLContext, filename: String): DataFrame = {
        val doubleFields = Array("ActionGeo_Lat", "ActionGeo_Long")
        val df = sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "false")
            .option("delimiter", "\t")
            .option("inferSchema", "true") // Automatically infer data types
            .schema(StructType(
            cols.split("\t")
                .map(fieldName => {
                    if (!doubleFields.contains(fieldName))
                        StructField(fieldName, StringType, true)
                    else
                        StructField(fieldName, DoubleType, true)
                })))
            .load(filename)
//        df.select("ActionGeo_CountryCode").distinct().collect().foreach(println(_))
        df
//            .filter("ActionGeo_CountryCode = 'SY'")
            .select("ActionGeo_Lat", "ActionGeo_Long", "EventCode")
            .filter("ActionGeo_Lat is not null and ActionGeo_Long is not null") // to get rid of null data if any
            .filter("EventCode != '042'")
            .filter("EventCode != '010'")
            .filter("EventCode != '043'")
            .filter("EventCode != '020'")
            .filter("EventCode != '051'")
            .filter("EventCode != '040'")
            .filter("EventCode != '036'")
            .filter("EventCode != '190'")
            .filter("EventCode != '193'")
    }

    def saveDf(df: DataFrame, filename: String): Unit = {
        println("Saving" + df.count() + "records..")
        val toSave = df
            .select("ActionGeo_Lat", "ActionGeo_Long", "EventCode")
            .map(x => (x.getDouble(0), x.getDouble(1), x.getString(2)))
            .coalesce(1)
            .saveAsTextFile(filename)
    }

    def getNormalized(sc: SparkContext, sqlContext: SQLContext, work: RDD[Tuple4[Double, Double, String, Int]], eventTypes: Array[String]): DataFrame = {

        import org.apache.spark.sql.Row;
        import org.apache.spark.sql.types.{StructType, StructField, StringType};
//        val typesList = eventTypes.toList
//        val pairs = (for (x <- typesList; y <- typesList) yield (x, y)).filter(t => t._1 < t._2)

//        val schemaString = "et1 et2"
//        val typesSchema =
//            StructType(
//                schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
//        val typesDf = sqlContext.createDataFrame(sc.parallelize(pairs.map(t => Row.fromTuple(t))), typesSchema)
//        typesDf.registerTempTable("types")

        val rows = work
            .map(t => ( (t._3, t._4), 1))
            .reduceByKey((x, y) => x + y)
            .map(t => (t._1._2, t._1._1, t._2)) // clusterid, eventcode, count
            .map(t => {
            val arr = Array.fill[Int](eventTypes.length)(0)
            arr(eventTypes.indexOf(t._2)) = t._3
            (t._1, arr)
        })
            .reduceByKey(
                (x, y) => {
                    (x, y).zipped.map(_ + _)
                }
            )
            .map(t => t._1 +: t._2.toSeq)
            .map(seq => Row.fromSeq(seq))
        val normSchema = StructType(
            ("cluster_id" +: eventTypes).map(fieldName => StructField(fieldName, IntegerType, true)))
        val normDf = sqlContext.createDataFrame(rows, normSchema)
        //        normDf.show(100)
        normDf
    }

    def getTransaction(norm: DataFrame, eventTypes: Array[String]): RDD[Array[String]] = {
        norm.
            drop("cluster_id")
            .map(row => row.toSeq.zipWithIndex.filter(t => t._1 != 0).map(t => eventTypes(t._2)).toArray)
    }

    def getTransactionWithId(norm: DataFrame, eventTypes: Array[String]): RDD[(Int, Array[String])] = {
        norm
            .map(row => (row.getInt(0),row.toSeq.slice(1, row.length).zipWithIndex.filter(t => t._1 != 0).map(t => eventTypes(t._2)).toArray))
    }

    def calcRules(transactions: RDD[Array[String]], minSupport: Double, numPartitions: Int): RDD[(Long, Rule[String])] = {

        def toFlatTuple(tuple: Tuple2[Long, Array[String]]): TraversableOnce[Tuple2[Long, String]] = {
            var result: Array[Tuple2[Long, String]] = Array()
            val id = tuple._1
            for (rl <- tuple._2) {
                result :+= (id, rl)

            }

            return result

        }

        import org.apache.spark.mllib.fpm.FPGrowth
        val fpg = new FPGrowth()
            .setMinSupport(minSupport)
            .setNumPartitions(numPartitions)
        transactions.cache()
        val model = fpg.run(transactions)

//        model.freqItemsets.collect().foreach { itemset =>
//            println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
//        }

        val minConfidence = 0.8

        val rules = model.generateAssociationRules(minConfidence)

        //---------------------------------------------------------------

        val rules_pairs = rules.zipWithUniqueId().map(r => Tuple2(r._2, r._1))
        rules_pairs.sortByKey()

        val formatter = new DecimalFormat("#.##")
        rules_pairs.map(rule => rule._1.toString + ": " + rule._2.antecedent.mkString("[", ",", "]") + "=>" + rule._2.consequent.mkString("[", ",", "],") + formatter.format(rule._2.confidence))
            .coalesce(1).saveAsTextFile("/usr/local/fproject/res/output/rulesZip")

        val antTuples = rules_pairs.map(s => (s._1, s._2.antecedent)).flatMap(s => toFlatTuple(s)).sortByKey()
        val conseqTuples = rules_pairs.map(n => (n._1, n._2.consequent)).flatMap(n => toFlatTuple(n)).sortByKey()
        val confidence = rules_pairs.map(s => (s._1, s._2.confidence))

        antTuples.coalesce(1).saveAsTextFile("/usr/local/fproject/res/output/ant")
        conseqTuples.coalesce(1).saveAsTextFile("/usr/local/fproject/res/output/conseq")
        confidence.coalesce(1).saveAsTextFile("/usr/local/fproject/res/output/confid")

        rules_pairs
    }

    def main(args: Array[String]): Unit = {

        val path: Path = Path("/usr/local/fproject/res/output")
        Try(path.deleteRecursively())

        val start = System.currentTimeMillis()

        val (sc, sqlContext) = initSc()
        val df = getCoords(sc, sqlContext, dataFiles)

        df
            .map(t => (t.getString(2), 1))
            .reduceByKey((x,y)=> x+y)
            .sortBy(_._2)
            .foreach(t => println(t))
        df.printSchema()
//        saveDf(df, "res/output/event_types")
        //        sc.stop()
        //        System.exit(0)
        //        for big dataset
        //        val eps = 0.1578505044669595
        //        val minPts = 468

        val eps = 0.470635222308477/100
        val minPts = 282/10
        val maxPartitionSize = 100
        val count = df.count()
        val dbscan = new DBScan()
            .setEpsilon(eps)
            .setMinPts(minPts)
//            .setMaxPartitionSize(count.toInt / sc.defaultParallelism)
            .setPPD(sc.defaultParallelism * 6)
        val data = df
            .map(r => Array(r.getDouble(0), r.getDouble(1)))
            .map(t => Vectors.dense(t))
            .repartition(sc.defaultParallelism * 3)
        println(count + " points to cluster") // TODO add info about cluster id using pointMap to collection with event types
        val model = dbscan.run(sc, data)
        println(model.numOfClusters + " were derived")
        model.points.coalesce(1).saveAsTextFile("res/output/clusters")

        val clusteringFinished = System.currentTimeMillis()
        println("Time consumed to clustering: " + (clusteringFinished - start) + " ms" )

        val pointsTuple = model.points.collect()
        val pointsMap: mutable.HashMap[Tuple2[Double, Double], Int] = mutable.HashMap()

        pointsTuple
            .foreach(p => pointsMap.put((p.vec(0), p.vec(1)), p.clusterId))
        println(pointsMap.size)

        df
            .select("ActionGeo_Lat", "ActionGeo_Long", "EventCode")
            .map(x => (x.getDouble(0), x.getDouble(1), x.getString(2), pointsMap((x.getDouble(0), x.getDouble(1)))))
            .coalesce(1)
            .saveAsTextFile("res/output/event_types")

        val work = df
            .map(r => (r.getDouble(0), r.getDouble(1), r.getString(2), pointsMap.get(r.getDouble(0), r.getDouble(1)).get))
            .filter(t => t._4 != 0)

        val eventTypes = work.map(t => t._3).distinct().collect().sorted
        val norm = getNormalized(sc, sqlContext, work, eventTypes)

        val trans = getTransaction(norm, eventTypes)

        val rulesStarted = System.currentTimeMillis()
        val rules = calcRules(trans, 0.2, sc.defaultParallelism)
        val rulesFinished = System.currentTimeMillis()
        println("Time consumed to rules derivation: " + (rulesFinished - rulesStarted) + " ms")

        getTransactionWithId(norm, eventTypes)
            .map(t=> (t._1, t._2.toSeq))
            .coalesce(1)
            .saveAsTextFile("res/output/transactions")
        sc.stop()

        val finish = System.currentTimeMillis()
        println("Total time " + (finish - start) + " ms")
    }
}
