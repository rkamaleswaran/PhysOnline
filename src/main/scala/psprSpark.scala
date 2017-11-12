///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//// scalastyle:off println
//package org.apache.spark.examples.sql.streaming

import java.io.PrintWriter
import java.sql.Timestamp
import javax.xml.bind.annotation.adapters.CollapsedStringAdapter
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode.Complete
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import scala.collection.Seq
import scala.util.matching.Regex
import scala.collection.mutable.LinkedHashMap
import scala.collection.JavaConverters._
import com.rabbitmq.client._
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.Connection
import com.rabbitmq.client.Channel
import com.rabbitmq.client.QueueingConsumer
import org.apache.spark.streaming.rabbitmq.RabbitMQUtils
import java.io.{BufferedReader, FileWriter, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import breeze.linalg.squaredDistance
import org.apache.spark.mllib.util
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.receiver._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.impl.StaticLoggerBinder

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Matrix => MX, Matrices => MS, Vectors}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator, CrossValidatorModel}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import scala.collection.JavaConverters._
import org.apache.spark.sql.functions.{concat, lit}
import com.mongodb.spark.config._
import com.mongodb.spark._


object PSPRwindow {

  def main(args: Array[String]) {

    //RabbitMQ things
    val host = "127.0.0.0"
    val queueName = "queueName"
    val userName = "user"
    val password = "password"
    val mongoDB = "mongodb://localhost:27017/psprDB.PAFData"


    //configure spark properties
    val sparkConfig = new SparkConf()
      .setAppName("PSPRwindow")
      .setIfMissing("spark.master", "local[*]")
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.dynamicAllocation.enabled","false")

    val ssc =  new StreamingContext(sparkConfig, Seconds(5))
    val sc = SparkContext



  val receiverStream = RabbitMQUtils.createStream(ssc, Map(
    "hosts" -> host,
    "queueName" -> queueName,
    "userName" -> userName,
    "password" -> password
  ))

    val CreatedSymbols = ssc.sparkContext.longAccumulator("My Accumulator")

    receiverStream.start()
    println("Started the Receiver Stream")

    receiverStream.foreachRDD(rdd => {
      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf)
        .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")

      import spark.implicits._

      //Read configureation for reading the MongoDB table
      val readConfig = ReadConfig(Map("uri" -> mongoDB))
      val info = MongoSpark.load(spark, readConfig)
                          .printSchema()

      if (!rdd.isEmpty()) {
        val Count = rdd.count()
        println("count is " + Count)
      //select column that is the array of datapoints and read it into a list and then into a dataframe

	  val t1 = System.nanoTime
      println("...Reading data frame... ")
        //Schema for PAFData
        val schema = StructType(
            StructField("label", StringType, true) ::
            StructField("id", StringType, true) ::
            StructField("sid", StringType, true) ::
            StructField("data",StringType, true) :: Nil)



        val DF1a = spark.read
          .schema(schema)
          .option("wholeTextFile", true).option("mode", "PERMISSIVE")
          .json(rdd)

        val toArray = udf((b: String) => b.split(",").map(_.toDouble))

        println("...Flattening data... ")

        val DF1 = DF1a.withColumn("data", toArray(col("data")))

        //flist for PAFData
        val fList = DF1.select($"id", $"sid", $"label", posexplode($"data")).withColumn("data_flat", $"col").drop("col")

        println("...Smoothing data... ")

        //Smoothing data with moving average
        val wSpec1 = Window.partitionBy("id").rowsBetween(-16, 0)

        val fL = fList.withColumn("movingAvg", avg(fList("data_flat")).over(wSpec1))
        fL.createOrReplaceTempView("df")

        println("...Calutlating quantiles...")

        //df2 for PAFData
        val df2 = spark.sql("select id, percentile_approx(movingAvg, array(0.2, 0.4, 0.6, 0.8D)) as quantiles from df group by id order by id")

        val jDF = df2.join(DF1, Seq("id"))

        println("...Creating joined table...")

	
        val jDF2 = jDF.select($"id", $"sid", $"label",  $"quantiles", posexplode($"data")).select($"id", $"sid", $"label",  $"quantiles", $"pos" , $"col".cast("double").as("data_flat"))

        println("...Converting data to symbols...")

        val numToSAX = when( $"data_flat" === null, null ).otherwise(
          when($"data_flat" < $"quantiles".getItem(0), "A" ).otherwise(
            when($"data_flat" >= $"quantiles".getItem(0) && $"data_flat" < $"quantiles".getItem(1), "B").otherwise(
              when($"data_flat" >= $"quantiles".getItem(1) && $"data_flat" < $"quantiles".getItem(2), "C").otherwise(
                when($"data_flat" >= $"quantiles".getItem(2) && $"data_flat" < $"quantiles".getItem(3), "D").otherwise(
                  "E"
                )
              )
            )
          )
        )

        //Calling function to create data
        val newPSPR = jDF2.withColumn("SAX", numToSAX)
        //newPSPR.show
        //Idenetify the PTPs
        val lPTPs = 6
        val nPTPs = 7

        println("...Generating symbol sequences...")

        for( a <- lPTPs to nPTPs){

          //create window
          val w= org.apache.spark.sql.expressions.Window.partitionBy("id", "sid").orderBy("id", "sid", "pos").rowsBetween(0,a-1)

          //create the new DF with window
          val olSAX = newPSPR.withColumn("olSAXw", collect_list("SAX").over(w))

          //println("...Showing olSAX...")
          //olSAX.show()
          //Set up DF
                 //Set 
        val SymCols= olSAX.select(($"label" +: $"sid" +: $"id" +: Range(0, a).map(idx => $"olSAXw"(idx) as "s" + (idx + 1)):_* ))
        val ncol = SymCols.columns.slice(0,a+2).toSeq
        val nseqCols = ncol.map(name => col(name))
        
        val sumSymbols= SymCols.groupBy((nseqCols :+ col("s" + a)):_*).count.withColumn("transitionCount", $"count").drop("count")

          //println("...showing sumSymbols...")
          //sumSymbols.show()
          //convert letter in CSi into strings for comparsion
          val Symbols = SymCols.groupBy(nseqCols:_*).count.withColumn("totalCount", $"count").drop("count")

          //pivot on sequence
          val pivotLoop= Symbols.join(sumSymbols, ncol)
		  .withColumn("prob", $"transitionCount" / $"totalCount" )
		  .groupBy(nseqCols:_*).pivot(("s" + a), Seq("A","B","C","D","E")).agg(first($"prob"))
		  .withColumn("SEQ",concat((nseqCols.slice(3, a+2)):_*) )
		  .withColumn("HEX", hex($"SEQ")).drop( (ncol.slice(3, a+2)):_* )
          println("...Writing PTP" + a.toString + " to MongoDB...")
          //pivotLoop.show()

          //read reference data from PAFepisode in MongoDB
          val r2 = "mongodb://localhost:27017/psprDB.PAFEpisode" + a.toString
          //val REFdf = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", r2).load()
          val readConfig2 = ReadConfig(Map("uri" -> r2))
          val PafRef = MongoSpark.load(spark, readConfig2).drop("_id")

          //PafRef.show()
		  
          println("...Creating vectors from data...")
          //Create Vectors
          val assemblerPAF = new VectorAssembler().setInputCols(Array("A","B","C","D","E")).setOutputCol("VectPAFPTP" + a.toString)
          val pivtVectPAF = assemblerPAF.transform(pivotLoop.na.fill(0)).drop("A","B","C","D","E")
          pivtVectPAF.createOrReplaceTempView("dfPTP" + a.toString)

          val assemblerREF = new VectorAssembler().setInputCols(Array("A","B","C","D","E")).setOutputCol("VectPAFPTPep" + a.toString)
          val pivtVectREF = assemblerREF.transform(PafRef.na.fill(0)).drop("A","B","C","D","E")
          pivtVectREF.createOrReplaceTempView("dfPTPep" + a.toString)

          val euclidean = udf((v1: Vector, v2: Vector) => Vectors.sqdist(v1, v2))

          val t1 = spark.sql("select * from dfPTP" +  a.toString )
          val t2 = spark.sql("select * from dfPTPep" +  a.toString )

          val jTab = t1.join(t2, Seq("HEX", "SEQ"))
          val jnTab = jTab.withColumn("dist" + a.toString, sqrt(euclidean( col("VectPAFPTP" + a.toString) , col("VectPAFPTPep" + a.toString) ))).drop("VectPAFPTP" + a.toString, "VectPAFPTPep" + a.toString)

          println("showing jnTab")
          //jnTab.show()
          jnTab.createOrReplaceTempView("jnPTP" + a.toString)

//          //save to DB
//          val uri = "mongodb://localhost:27017/psprDB.PAFData" + a.toString
//          val writeConfig2 = WriteConfig(Map("uri"-> uri ))
//          val Save = MongoSpark.save(pivotLoop.write.mode("append"),writeConfig2)

        }

		println("Starting to merge all PTPs into single dataframe")

		for( a <- lPTPs to nPTPs-1){

			val t1 = spark.sql("select * from jnPTP" +  a.toString ).drop("HEX", "SEQ")
			val t2 = spark.sql("select * from jnPTP" +  (a+1).toString ).drop("HEX", "SEQ")

			val jTab = t1.join(t2, Seq("id", "sid", "label"))
			jTab.createOrReplaceTempView("jnPTP" + (a+1).toString)
		}

		val inputR = spark.sql("select * from jnPTP" + nPTPs.toString)
			
		val exprs = inputR.columns.slice(3, nPTPs+3).map( r => (sqrt(sum( r )) / count("id") ).as(r) )

		println("Starting GroupBy by label, id, and sid")
		
		val jal = inputR.groupBy("label", "id", "sid").agg(exprs.head, exprs.tail: _*)

		println("Starting descriptives calculations" + " Duration was: " + ((System.nanoTime - t1) / 1e9d) )
		
		//calculate and append descriptives to dataframe
		val wSpecDesc = Window.partitionBy("id", "sid").orderBy("sid")
		val fLDesc = fList.withColumn("mean", mean(fList("data_flat")).over(wSpecDesc)).withColumn("min", min(fList("data_flat")).over(wSpecDesc)).withColumn("max", max(fList("data_flat")).over(wSpecDesc)).withColumn("stdev", stddev(fList("data_flat")).over(wSpecDesc)).withColumn("kurt", kurtosis(fList("data_flat")).over(wSpecDesc)).withColumn("skew", skewness(fList("data_flat")).over(wSpecDesc)).withColumn("variance", variance(fList("data_flat")).over(wSpecDesc)).withColumn("sum", sum(fList("data_flat")).over(wSpecDesc))
		val finalDescriptivesPt = fLDesc.drop("pos", "data_flat").groupBy("label", "id", "sid").agg(first("mean").as("mean"), first("min").as("min"), first("max").as("max"), first("stdev").as("stdev"), first("kurt").as("kurt"), first("skew").as("skew"), first("variance").as("variance"), first("sum").as("sum"), (first("max") - first("min")).as("range")    )

		val finalDB = finalDescriptivesPt.join(jal, Seq("label", "id", "sid"))

		val ncol = finalDB.columns.slice(3,nPTPs+12).toArray

		println("Completed descriptives now assembling final vector" + " Duration was: " + ((System.nanoTime - t1) / 1e9d))
		
		val asemblr = new VectorAssembler().setInputCols(ncol)
				  .setOutputCol("features")
		//
		//        val PAFout = assembler1.transform(pivot3)
		val inputSA = asemblr.transform(finalDB)
		//inputSA.cache
		//inputSA.show
    println("Completed transform assembling" + " Duration was: " + ((System.nanoTime - t1) / 1e9d))
		//val inclu = Array("P001", "P009", "P0011", "P0021", "P0031", "P0041", "P0061", "P0071")
    val inclu = Array("")
		val training = inputSA.filter(!$"id".isin(inclu:_*)).select($"label".cast("int"), $"features")
		val testing = inputSA.filter($"id".isin(inclu:_*)).select($"label".cast("int"), $"features")
		
		//training.show()

		
		val model = CrossValidatorModel.load("/home/spark/psprStreaming/out/myRandomForestClassificationModel")
    println("loaded model " + " Duration was: " + ((System.nanoTime - t1) / 1e9d))

        // PTP 2- 6

		val predictions = model.transform(training)
		// predictions.select( "label", "prediction").show(5)

        println("generated predictions " + " Duration was: " + ((System.nanoTime - t1) / 1e9d))

		val evaluator = new MulticlassClassificationEvaluator()
		  .setLabelCol("label")
		  .setPredictionCol("prediction")
		  .setMetricName("accuracy")

        println("generated evaluator " + " Duration was: " + ((System.nanoTime - t1) / 1e9d))
		val accuracy = evaluator.evaluate(predictions)

        println("completed evaluation " + " Duration was: " + ((System.nanoTime - t1) / 1e9d))

		val TP = predictions.select("label", "prediction").filter("label = 1 and prediction = 1").count  
		val TN = predictions.select("label", "prediction").filter("label = 0 and prediction = 0").count  
		val FP = predictions.select("label", "prediction").filter("label = 0 and prediction = 1").count  
		val FN = predictions.select("label", "prediction").filter("label = 1 and prediction = 0").count  
		val total = predictions.select("label").count.toDouble

		val confusion: MX = MS.dense(2, 2, Array(TP, FN, FP, TN))

		val acc    = (TP + TN) / total  
		val precision   =  TP / (TP + FP).toDouble 
		val recall      = TP / (TP + FN).toDouble
		val sensitivity = TP / (TP + FN).toDouble
		val specificity = TN / (TN + FP).toDouble
		val F1        = 2/(1/precision + 1/recall) 

	
	val duration = (System.nanoTime - t1) / 1e9d
	val result = "Epoch: " + System.currentTimeMillis() + " Duration of Job was: " + ((System.nanoTime - t1) / 1e9d) +  " Accuracy : " + accuracy + " Sen: " + sensitivity + " Sps: " + specificity + " F1: " + F1
	
	new PrintWriter("./out/latestresult.txt") { write(result); close }

	val fw = new FileWriter("./out/results.txt", true) ; 
	fw.write( "\r\n" + result)
	fw.close();
    println(result)
		
		
		
//        val writeConfig2 = WriteConfig(Map("uri"-> "mongodb://localhost:27017/psprDB.PAFData"))
//        val Save = MongoSpark.save(pivot3.write.mode("append"),writeConfig2)


        println("..............................................................")
        println("..............................................................")
        println(".......................Batch finished.........................")
        println("..............................................................")
        println("..............................................................")
    }
      else
        println("Queue is empty. Waiting to be refilled...")
    })


    ssc.start()
    ssc.awaitTermination()


  }
}

