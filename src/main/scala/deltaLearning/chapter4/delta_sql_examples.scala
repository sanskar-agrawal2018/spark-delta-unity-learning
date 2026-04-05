package deltaLearning.chapter4

import deltaLearning.utils.operations
import deltaLearning.utils.Deltautils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Small examples showing:
 * 1) Creating a Delta table (nested schema) via SQL
 * 2) Inserting sample event rows using SQL (named_struct, array)
 * 3) Inserting the same data in multiple separate transactions
 * 4) Reading the Delta table as a stream starting from a specific version
 */
object CreateEventsTableSQL_1 extends App with operations {
  val utils = new Deltautils()
  val spark = utils.getSparkSession()
  spark.sparkContext.setLogLevel("ERROR")

  val path = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/events_sql_table"

  // Create table with nested types

  val dropSql = s"DROP TABLE IF EXISTS delta.`${path}`"
  spark.sql(dropSql)
  println("Dropped existing table if it existed.")
  val createSql = s"""
    CREATE TABLE IF NOT EXISTS delta.`${path}` (eventId STRING,
    |  event_time TIMESTAMP,
    |  user STRUCT<id:INT, name:STRING>,
    |  device STRUCT<type:STRING, os:STRING, browser:STRING>,
    |  transactions ARRAY<STRUCT<txnId:STRING, amount:DOUBLE, currency:STRING>>,
    |  success BOOLEAN
    |)
    |USING DELTA
    |LOCATION '${path}'
    |""".stripMargin



  spark.sql(createSql)


  println(s"Created Delta table at: ${path}")

  spark.sql(s"DESCRIBE EXTENDED delta.`${path}`").show(truncate = false)
  utils.stopTheThread()
}

object InsertSampleEventsSQL extends App with operations {
  val utils = new Deltautils()
  val spark = utils.getSparkSession()
  spark.sparkContext.setLogLevel("ERROR")

  val path = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/events_sql_table"

  // Three sample event inserts. Each INSERT INTO is its own transaction.
  val insert1 = s"""
    |INSERT INTO delta.`${path}` VALUES (
    |  'e1',
    |  '2026-01-29T10:15:30Z',
    |  named_struct('id', 101, 'name', 'Amit'),
    |  named_struct('type','mobile','os','android','browser',null),
    |  array(named_struct('txnId','t1','amount',250.75,'currency','INR')),
    |  true
    |)
    |""".stripMargin

  val insert2 = s"""
    |INSERT INTO delta.`${path}` VALUES (
    |  'e2',
    |  '2026-01-29T10:15:32Z',
    |  named_struct('id', 102, 'name', 'Sara'),
    |  named_struct('type','web','os',null,'browser','chrome'),
    |  array(),
    |  false
    |)
    |""".stripMargin

  val insert3 = s"""
    |INSERT INTO delta.`${path}` VALUES (
    |  'e3',
    |  '2026-01-29T10:15:35Z',
    |  named_struct('id', 103, 'name', 'Rahul'),
    |  named_struct('type','desktop','os','linux','browser',null),
    |  array(named_struct('txnId','t2','amount',9999.99,'currency','INR')),
    |  true
    |)
    |""".stripMargin

  spark.sql(insert1)
  println("Inserted row e1")
  spark.sql(insert2)
  println("Inserted row e2")
  spark.sql(insert3)
  println("Inserted row e3")

  println("Current table contents:")
  spark.read.format("delta").load(path).show(false)

  utils.stopTheThread()
}

object InsertSameDataMultipleTransactions extends App with operations {
  val utils = new Deltautils()
  val spark = utils.getSparkSession()
  spark.sparkContext.setLogLevel("ERROR")

  val path = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/events_sql_table"

  // We'll insert the same sample row three times, each as a separate transaction.
  val baseInsert = s"""
    |INSERT INTO delta.`${path}` VALUES (
    |  'txn_event',
    |  '2026-01-27T10:15:32Z',
    |  named_struct('id', 999, 'name', 'TxUser'),
    |  named_struct('type','api','os','linux','browser',null),
    |  array(named_struct('txnId','tx1','amount',42.0,'currency','USD')),
    |  true
    |)
    |""".stripMargin

  for (i <- 1 to 3) {
    // each spark.sql call is a separate transaction on the delta table
    spark.sql(baseInsert)
    println(s"Committed transaction #${i} inserting txn_event")
  }

  println("After multiple transactions, row count for txn_event:")
  val df = spark.read.format("delta").load(path).filter("eventId = 'txn_event'")
  df.show(false)

  utils.stopTheThread()
}





object ReadDeltaWithStartingVersion extends App with operations {
  val utils = new Deltautils()
  val spark = utils.getSparkSession()
  spark.sparkContext.setLogLevel("ERROR")

  val path = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/events_sql_table"

  // "startingVersion" acts as an initial processing position for Delta streaming reads.
  // Set it to 0 to start from the first table version, or a specific integer to pick a snapshot.


  val readStream = spark.readStream
    .format("delta")
    .option("withEventTimeOrder", "true")
    .option("maxFilesPerTrigger" ,"1")// for demo, use event_time as watermark
    .load(path)

  // For demo we use foreachBatch to print each micro-batch.
  def foreachBatchFn(batchDF: org.apache.spark.sql.DataFrame, batchId: Long): Unit = {
    println(s"--- Batch: ${batchId} ---")
//    batchDF.show(false)
    batchDF
      .groupBy(col("eventId"), window(col("event_time"), "1 hour"))
      .agg(count("*").alias("count"))
      .show(false)

  }

  val query = readStream
    .withWatermark("event_time", "1 hour") // use event_time as watermark
    .groupBy(col("eventId"), window(col("event_time"), "1 hour"))
    .agg(count("*").alias("count"))
    .writeStream
    .format("console")
    .outputMode("append")
    .start()

//    .option("checkpointLocation", path + "/_checkpoints/read_with_starting_version")
//    .option("maxFilesPerTrigger", 1) // for demo, process one file at a time


  println("Started streaming read (use Ctrl+C to stop in REPL). Waiting for a single trigger...")
//  query.processAllAvailable()
//  query.stop()
  query.awaitTermination()
  utils.stopTheThread()
}



