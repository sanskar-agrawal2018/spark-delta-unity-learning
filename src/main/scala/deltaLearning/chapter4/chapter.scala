package deltaLearning.chapter4


import deltaLearning.utils.operations
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, e, explode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress}



class MyStreamingListener(spark:SparkSession) extends StreamingQueryListener {

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    println(s"Query started: ${event.id}")
    println(s"RunId: ${event.runId}")

  }


  override def onQueryIdle(event: StreamingQueryListener.QueryIdleEvent): Unit = {
    //      println(s"Query idle: ${event.id}")
    //      println(s"Waiting ")
    println(s"Query Waiting started")

    for(q <- spark.streams.active) {

      println(s"Current status of the query: ${q.status}")
      println(s"Current progress of the query: ${q.lastProgress}")
      q.stop()
    }


    println(s"Current time is: ${java.time.LocalTime.now()}")
    println(event.id, event.runId, event.timestamp,event.json)
    println(s"Query Waiting ended")
  }
  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val progress: StreamingQueryProgress = event.progress

    println("----- Batch Completed -----")
    println(s"Batch ID: ${progress.batchId} sleep started")
    // Sleep for 10 min simulate long processing
    //      Thread.sleep(1000*60*10)

    println(s"Batch ID: ${progress.batchId} sleep ended")


    println(s"Progress: ${progress}")
    println(s"Batch ID: ${progress.batchId}")
    println(s"Input Rows: ${progress.numInputRows}")
    println(s"Input Rows/sec: ${progress.inputRowsPerSecond}")
    println(s"Processed Rows/sec: ${progress.processedRowsPerSecond}")
    println(s"Batch Duration: ${progress.durationMs}")



  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    println(s"Query terminated: ${event.id}")
    println(s"Exception: ${event.exception}")
  }
}


object chapter4_read_delta_and_writeStream_delta  extends App with operations {

  import deltaLearning.utils.Deltautils

  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()

  val readSteam= spark.readStream
    .format("delta")
    .option("maxfilespertrigger",1)
    .load("/home/sanskar/Project/Spark-test/spark-3.5-test/output/events")

  spark.sparkContext.setLogLevel("ERROR")


  def func1(df:org.apache.spark.sql.DataFrame, batchId:Long):Unit={
    println(s"Batch Id: $batchId started")
    println(s"Number of records in this batch: ${df.count()}")
//    df.show(truncate=false)
    df.write.format("delta").mode("append").save("/home/sanskar/Project/Spark-test/spark-3.5-test/output/events_silver")

    println(s"Batch Ended")
//    func2()
  }

  var countWaiting=0


  // Register Listener
  spark.streams.addListener(new MyStreamingListener(spark))

  val query=readSteam
    .writeStream
    .format("delta")
    .option("checkpointLocation","/home/sanskar/Project/Spark-test/spark-3.5-test/output/events_silver/_checkpoints/stream1")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .foreachBatch(func1 _)
    .start()


  def func2():Unit={
    println("Query Logs Started")
    println(query.lastProgress)
  }




//  while (query.isActive){
//    func2()
//    Thread.sleep(10000)
//  }


  println("Waiting for the streaming to finish...")
  query.awaitTermination()
  println("Streaming stopped.")



}





object chapter4_read_delta_and_writeStream_parquet  extends App with operations {

  import deltaLearning.utils.Deltautils

  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()

  val readSteam= spark.readStream
    .format("delta")
    .option("maxfilespertrigger",1)
    .load("/home/sanskar/Project/Spark-test/spark-3.5-test/output/events")

  val query=readSteam
    .writeStream
    .queryName("write_to_parquet")
    .format("parquet")
    .option("checkpointLocation","/home/sanskar/Project/Spark-test/spark-3.5-test/output/events_silver_parquet/_checkpoints/stream1")
    .outputMode("append")
    .start("/home/sanskar/Project/Spark-test/spark-3.5-test/output/events_silver_parquet")

  query.awaitTermination()


}


object readDeltaTable extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()

  val deltaTable=getDeltaTable("/home/sanskar/Project/Spark-test/spark-3.5-test/output/events_silver")

  println(deltaTable.toDF.count())
//  deltaTable.toDF.show(truncate=false)

}


/**
 * Stream read from a Delta events table, join with a static Delta table,
 * and write the joined result as a Delta table in append mode.
 *
 * - streaming source: /output/events
 * - static table: /output/events_sql_table
 * - output: /output/events_joined_static
 */
object chapter4_stream_join_static_delta extends App with operations {
  import deltaLearning.utils.Deltautils

  val utils = new Deltautils()
  val spark = utils.getSparkSession()
  spark.sparkContext.setLogLevel("ERROR")

  val streamPath = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/events"
  val staticUserPath = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/user_profile_sql_table"
  val outputPath = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/events_stream_joined_static_user"

  // streaming read from delta


//  readDF.show(3,truncate=false)





  val eventsStream = spark.readStream
    .format("delta")
    .load(streamPath)
//
//  // static/batch read of delta table to join with

  val staticDF = spark.read.format("delta").load(staticUserPath)

  // cache static side for repeated use
  staticDF.cache()

//
//  // perform a stream-static left join on eventId
  val joined = eventsStream.join(staticDF, eventsStream("user")("id") === staticDF("userId"), "left_outer")
    .drop("userId")
    .withColumn("ingestion_time", current_timestamp())

  spark.streams.addListener(new MyStreamingListener(spark))

//
//  // write the joined output as delta in append mode with checkpointing
  val query = joined.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", outputPath + "/_checkpoints/stream_join")
    .start(outputPath)
//
  println("Started stream join job. Waiting for termination...")
  query.awaitTermination()

}
