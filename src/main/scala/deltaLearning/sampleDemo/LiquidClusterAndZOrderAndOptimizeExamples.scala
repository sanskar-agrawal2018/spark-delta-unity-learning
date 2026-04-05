package deltaLearning.sampleDemo

import deltaLearning.utils.operations
import deltaLearning.CollectActiveDeltaLogMetadata._
import deltaLearning.utils.Deltautils
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.util.Random
import deltaLearning.ReadDeltaTransactionLogs._

object CustomerZOrderDemoSupport {
  val workspaceRoot = "/home/sanskar/Project/Spark-test/spark-3.5-test"
  val basePath = s"$workspaceRoot/output/sample_demo/customer_zorder"
  val tablePath = s"$basePath/customers_delta_liquid_cluster"
  private val recordsPerTransaction = 1
  // simple transaction struct used as element of the transactions array
  case class Tx(transaction_id: Long, amount: Double)

  // Body dimensions struct and Attributes struct
  // Attributes.hobbies -> Array[String]
  // Attributes.body -> Struct { height: Double, weight: Double }
  case class BodyDimension(height: Double, weight: Double)
  case class Attributes(hobbies: Seq[String], body: BodyDimension)

  def createSparkSession(appName: String): SparkSession = {
    val spark = new Deltautils().getSparkSession()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.databricks.delta.optimize.maxFileSize",500000)
    spark.conf.set("spark.sql.shuffle.partitions", 4)
//    not working
//    spark.conf.set("spark.databricks.delta.targetFileSize", "4788190.0")
//    not working
//    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")  //
    spark.conf.set("spark.databricks.delta.optimizeWrites.enabled", "true")

    println(s"App Name: $appName")
    println("Configured spark.databricks.delta.optimize.maxFileSize=12288 bytes")
    println("This is a byte limit, not a strict row limit. The payload column is widened so OPTIMIZE/Z-ORDER stays around ~10 rows per file.")
    spark
  }

  def resetDemoArea(spark: SparkSession): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(basePath), true)
  }

  def appendCustomers(spark: SparkSession, startCustomerId: Int, transactionCount: Int, runLabel: String): Unit = {
    import spark.implicits._

    (0 until transactionCount).foreach { txnId =>
      val rows = (0 until recordsPerTransaction).map { offset =>
        val customerId = txnId  + (transactionCount * offset )
//        val customerId=1+(88+1)
        val region = Seq("north", "south", "east", "west")(customerId % 4)
        val city = Seq("Delhi", "Mumbai", "Pune", "Chennai", "Bengaluru")(customerId % 5)
        val segment = if (customerId % 2 == 0) "retail" else "enterprise"
        val payload = ("customer_payload_" + customerId + "_")+"payload"

        // determine number of transactions for this customer based on customerId % 100
        // then cap to 100 to limit the maximum
        val numTx = math.min(1000 + ((1+98) % 100), 10)

        // generate transactions as an array of Tx(transaction_id, amount)
        val transactions = (0 until numTx).map { i =>
          // make a deterministic-ish transaction id based on customerId to avoid collisions
          val txId = customerId.toLong * 1000L + i.toLong
          val amount = BigDecimal(Random.nextDouble() * 1000.0).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
          Tx(txId, amount)
        }

        // attributes: previously a Map[String,String]; now convert to a Struct with hobbies and body dimensions
        val attrSize = ((1+8) % 10) + 1 // yields 1..10 entries
        // hobbies: variable-length array of hobby names
        val hobbies: Seq[String] = (0 until attrSize).map(i => s"hobby_${(1 + i) % 7}")
        // body dimensions as a nested struct
        val body = BodyDimension(
          150.0 + (customerId % 50), // height in cm (example)
          50.0 + (customerId % 80)   // weight in kg (example)
        )
        val attributes = Attributes(hobbies, body)

        (
          customerId,
          s"customer_$customerId",
          city,
          region,
          segment,
          (customerId % 5 + 1) * 1000,
          runLabel,
          payload,
          transactions,
          attributes
        )
      }

      rows.toDF("customer_id", "customer_name", "city", "region", "segment", "credit_limit", "ingestion_run", "payload", "transactions", "attributes")
        .repartition(1)
        .write
        .format("delta")
        .mode(SaveMode.Append)
        .save(tablePath)

      println(s"Write transaction ${txnId + 1}/$transactionCount for run=$runLabel")
    }
  }

  def runOptimizeAndPrintStats(spark: SparkSession,label:String): Seq[String] = {
    val beforeFiles = currentActiveDeltaParquetFiles(spark, tablePath)
    println(s"\n=== $label ===")
    println(s"Active parquet files before run: ${beforeFiles.size}")


    val metrics=spark.sql(s"OPTIMIZE delta.`$tablePath`")


    println(s"Compaction metrics for $label:")
    metrics.show(truncate = false)
    metrics.printSchema()

    val afterFiles = currentActiveDeltaParquetFiles(spark, tablePath)
    val newlyCreatedFiles = afterFiles.diff(beforeFiles)
    val removedFiles = beforeFiles.diff(afterFiles)

    println(s"Active parquet files after run: ${afterFiles.size}")
    println(s"New parquet files after run: ${newlyCreatedFiles.size}")
    println(s"Parquet files removed by run: ${removedFiles.size}")

//    printTableStats(spark, s"Stats after $label")
    afterFiles
  }

  def runZOrderAndPrintStats(spark: SparkSession, label: String): Seq[String] = {
    val beforeFiles = currentActiveDeltaParquetFiles(spark, tablePath)
    println(s"\n=== $label ===")
    println(s"Active parquet files before run: ${beforeFiles.size}")

    val metrics = DeltaTable.forPath(spark, tablePath)
      .optimize()
      .executeZOrderBy("region")

    println(s"OPTIMIZE ZORDER metrics for $label:")
    metrics.show(truncate = false)

    val afterFiles = currentActiveDeltaParquetFiles(spark, tablePath)
    val newlyCreatedFiles = afterFiles.diff(beforeFiles)
    val removedFiles = beforeFiles.diff(afterFiles)

    println(s"Active parquet files after run: ${afterFiles.size}")
    println(s"New parquet files after run: ${newlyCreatedFiles.size}")
    println(s"Parquet files removed by run: ${removedFiles.size}")

//    printTableStats(spark, s"Stats after $label")
    afterFiles
  }

  def printTableStats(spark: SparkSession, label: String): Unit = {
    println(s"\n--- $label ---")
    val deltaTable = DeltaTable.forPath(spark, tablePath)

    spark.read.format("delta").load(tablePath)
      .orderBy("region")
      .show(20, truncate = false)

    deltaTable.detail()
      .select("numFiles", "sizeInBytes")
      .show(false)

    deltaTable.history(5)
      .select("version", "timestamp", "operation", "operationParameters", "operationMetrics")
      .show(false)

    currentParquetFilesWithSize(spark, tablePath)
      .take(12)
      .foreach { case (path, size) =>
        println(s"file=$path sizeBytes=$size")
      }
  }

  def currentActiveDeltaParquetFiles(spark: SparkSession, path: String): Seq[String] = {
    currentParquetFilesWithSize(spark, path).map(_._1)
  }

  def currentParquetFilesWithSize(spark: SparkSession, path: String): Seq[(String, Long)] = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
   println(fs)
    val base = new Path(path)
    if (!fs.exists(base)) {
      Seq.empty
    } else {
      fs.listStatus(base)
        .filter(status => status.isFile && status.getPath.getName.endsWith(".parquet"))
        .map(status => status.getPath.getName -> status.getLen)
        .sortBy(_._1)
        .toSeq
    }
  }
}

object understandApp extends App {
  import CustomerZOrderDemoSupport._

  val spark = createSparkSession("CustomerZOrderInitialDemo")

  val x=currentParquetFilesWithSize(spark, tablePath)
  println(x)
  val filesAfterFirstZOrder = runZOrderAndPrintStats(spark, "Fifth Z-ORDER")
  val filesAfterSecondZOrder = runZOrderAndPrintStats(spark, "Sixth Z-ORDER on same data")

//  println(s"Did active parquet file names change on second Z-ORDER: ${filesAfterFirstZOrder != filesAfterSecondZOrder}")
  spark.stop()
}

object CustomerZorderAgain extends App {

  def runZOrderMultipleTimes(spark: SparkSession, times: Int): Unit = {
    import CustomerZOrderDemoSupport._

    val spark = createSparkSession("CustomerZOrderInitialDemo")


    val n = times
    for (i <- 1 to  n) {
      runZOrderAndPrintStats(spark, s"${i}th Z-ORDER")
      println(s"------- Optimize ended for $i/$n times  --------")

    }
  }


  /**
   * Run for liquid clustering
  */
  def runCompactionMultipleTimes(spark: SparkSession, times: Int): Unit = {
    import CustomerZOrderDemoSupport._

    val spark = createSparkSession("CustomerZOrderInitialDemo")


    val n = times
    for (i <- 1 to  n) {
      runOptimizeAndPrintStats(spark, s"${i}th Compaction")
      println(s"------- Optimize ended for $i/$n times  --------")

    }
  }

  /**
   * Run a clustering operation multiple times. This is written to behave similarly to the
   * existing Z-ORDER helper routines in this file. For environments where Delta "liquid"
   * clustering is available, you can replace the call to executeZOrderBy with the
   * appropriate liquid clustering API. Here we use the Delta Optimize + Z-ORDER API as a
   * compatible, portable approximation for clustering / file reorganization.
   */


}



object CustomerZOrderInitialDemo extends App {

  import CustomerZOrderDemoSupport._

  val spark = createSparkSession("CustomerZOrderInitialDemo")

  resetDemoArea(spark)
  appendCustomers(spark, startCustomerId = 1, transactionCount = 12, runLabel = "initial-load")

//  printTableStats(spark, "Before first Z-ORDER")
//
//  val filesAfterFirstZOrder = runZOrderAndPrintStats(spark, "First Z-ORDER")
//  val filesAfterSecondZOrder = runZOrderAndPrintStats(spark, "Second Z-ORDER on same data")
//
//  val filesAfterThirdZOrder = runZOrderAndPrintStats(spark, "Third Z-ORDER")
//  val filesAfterFourthZOrder = runZOrderAndPrintStats(spark, "Fourth Z-ORDER on same data")

//  println(s"Did active parquet file names change on second Z-ORDER: ${filesAfterFirstZOrder != filesAfterSecondZOrder}")
  spark.stop()
}

object CustomerZOrderAppendDemo extends App {
  import CustomerZOrderDemoSupport._

  val spark = createSparkSession("CustomerZOrderAppendDemo")

  if (!DeltaTable.isDeltaTable(spark, tablePath)) {
    println(s"Delta table not found at $tablePath. Run CustomerZOrderInitialDemo first.")
    spark.stop()
    sys.exit(1)
  }

  appendCustomers(spark, startCustomerId = 1, transactionCount = 1 , runLabel = "append-load")
  printTableStats(spark, "After append and before re-running Z-ORDER")
//  val filesAfterFirstZOrder = runZOrderAndPrintStats(spark, "First Z-ORDER")
//  val filesAfterSecondZOrder = runZOrderAndPrintStats(spark, "Second Z-ORDER on same data")
//
//  val filesAfterThirdZOrder = runZOrderAndPrintStats(spark, "Third Z-ORDER")
//  val filesAfterFourthZOrder = runZOrderAndPrintStats(spark, "Fourth Z-ORDER on same data")


  spark.stop()
}


/**
 * Create the Delta table with the requested schema and set TBLPROPERTIES.
 */
object CreateCustomerDeltaTable extends App {
  import CustomerZOrderDemoSupport._

  val spark = createSparkSession("CreateCustomerDeltaTable")


  spark.sql(s"DROP TABLE IF EXISTS delta.`${tablePath}`")

  // Create table SQL with requested schema and set the delta.checkpoint.writeStatsAsStruct property
  val createSql = s"""
    CREATE TABLE IF NOT EXISTS delta.`$tablePath` (
    |  customer_id INT,
    |  customer_name STRING,
    |  city STRING,
    |  region STRING,
    |  segment STRING,
    |  credit_limit INT,
    |  ingestion_run STRING,
    |  payload STRING,
    |  transactions ARRAY<STRUCT<transaction_id:BIGINT, amount:DOUBLE>>,
    |  attributes STRUCT<hobbies:ARRAY<STRING>, body:STRUCT<height:DOUBLE, weight:DOUBLE>>
    |)
    |USING DELTA
    |LOCATION '$tablePath'
    |CLUSTERED BY (region,customer_id)
    |""".stripMargin
  println(createSql)

  println(s"Creating Delta table at: $tablePath")
  spark.sql(createSql)

  println("Table created - DESCRIBE EXTENDED:")
  spark.sql(s"DESCRIBE EXTENDED delta.`$tablePath`").show(truncate = false)

  spark.stop()
}


/**
 * Create a second Delta table and perform an Optimize + Z-ORDER on (region, customer_id)
 * to emulate liquid clustering behavior. This keeps the code portable across Delta runtimes.
 */
object CreateCustomerDeltaTableWithClustering extends App {
  import CustomerZOrderDemoSupport._

  val spark = createSparkSession("CreateCustomerDeltaTableWithClustering")

  val clusteringTablePath = s"${tablePath}"

def tableExists(spark: SparkSession, pathOrTable: String): Boolean = {
  try {
    // check metastore first (works for table names)
    if (spark.catalog.tableExists(pathOrTable)) {
      true
    } else {
      // fallback: check if a Delta table exists at the given path
      DeltaTable.isDeltaTable(spark, pathOrTable)
    }
  } catch {
    case _: Throwable => false
  }
}
  println(s"Table exist ${tableExists(spark,clusteringTablePath)}")
  spark.sql(s"DROP TABLE IF EXISTS delta.`$clusteringTablePath`").show()

  println(s"Table exist ${tableExists(spark,clusteringTablePath)}")
  val createClusteringSql = s"""
    CREATE TABLE IF NOT EXISTS delta.`$clusteringTablePath` (
    |  customer_id INT,
    |  customer_name STRING,
    |  city STRING,
    |  region STRING,
    |  segment STRING,
    |  credit_limit INT,
    |  ingestion_run STRING,
    |  payload STRING,
    |  transactions ARRAY<STRUCT<transaction_id:BIGINT, amount:DOUBLE>>,
    |  attributes STRUCT<hobbies:ARRAY<STRING>, body:STRUCT<height:DOUBLE, weight:DOUBLE>>
    |)
    |USING DELTA
    |LOCATION '$clusteringTablePath'
    |CLUSTER BY (region, customer_id)
    |""".stripMargin

  println(s"Creating Delta table for clustering at: $clusteringTablePath")
  spark.sql(createClusteringSql).show()
  println("Describe extended for clustering table:")
  spark.sql(s"DESCRIBE EXTENDED delta.`$clusteringTablePath`").show(truncate = false)

  spark.stop()
}


object PrintTheZOrderAndStats extends App with operations{
  import CustomerZOrderDemoSupport._

  val spark = createSparkSession("CustomerZOrderInitialDemo")
//  spark.sql(s"ALTER TABLE delta.`${tablePath}` ADD  COLUMNS (customer_id INT)")

//    spark.sql(s"ALTER TABLE delta.`${tablePath}` ADD  COLUMNS (customer_id_1 INT)")
//
  val df_prev=collectActiveFilesForTable(spark,tablePath)



  println(s"Count :- ${df_prev.count}")



  df_prev
    .select(col("path"),col("size"),col("stats_parsed.numRecords"),col("stats_parsed.minValues.customer_id"),col("stats_parsed.maxValues.customer_id"),col("stats_parsed.minValues.region"),col("stats_parsed.maxValues.region"))
    .orderBy(col("stats_parsed.minValues.customer_id")).show(truncate = false)
  import CustomerZorderAgain._
//  runZOrderMultipleTimes(spark,1)
  runCompactionMultipleTimes(spark,1)
//  runOptimie(spark,2)
//  runZOrderAndPrintStats(spark,1)
  // drop column consumr_id

  val deltaTable1 =DeltaTable.forPath(spark,tablePath)
//  deltaTable1.checkpoint()

  spark.sql(s"DESCRIBE EXTENDED delta.`${tablePath}`").show(truncate=false)
  deltaTable1.history().show(truncate=false)
  deltaTable1.history().printSchema()




  val df=collectActiveFilesForTable(spark,tablePath)



  println(s"Count :- ${df.count}")



  df
    .select(col("path"),col("size"),col("stats_parsed.numRecords"),col("stats_parsed.minValues.customer_id"),col("stats_parsed.maxValues.customer_id"),col("stats_parsed.minValues.region"),col("stats_parsed.maxValues.region"))
    .orderBy(col("stats_parsed.minValues.customer_id")).show(truncate = false)
//  df.show(truncate=false)stats
  df.select(col("stats"),col("stats_parsed")).printSchema()

  // Group files by size rounded to the nearest thousand
  val dfGrouped = df.withColumn("size_nearest_thousand", expr("round(size/1000.0)*1000").cast("long"))
    .groupBy("size_nearest_thousand")
    .agg(count("*").alias("file_count"), avg(col("size")).alias("average_size"),min(col("stats_parsed.numRecords")).alias("min_record_count"),max(col("stats_parsed.numRecords")).alias("max_record_count"),avg(col("stats_parsed.numRecords")).alias("average_count"),sum(col("size")).alias("total_size")

    )
//    ,collect_list(struct(col("stats_parsed.minValues.customer_id"),col("stats_parsed.maxValues.customer_id"),col("stats_parsed.minValues.region"),col("stats_parsed.maxValues.region"))).alias("customer_id_region_ranges"))
    .orderBy("size_nearest_thousand")

  println("Grouped by nearest thousand (size_nearest_thousand, file_count, total_size):")
  dfGrouped.show(100,truncate = false)

//  df.show(100,truncate = false )

  spark.stop()
}

object printCheckpoint  extends App {
  import io.delta.tables.DeltaTable
  import org.apache.spark.sql.delta.DeltaLog

  import CustomerZOrderDemoSupport._
  val spark = createSparkSession("CustomerZOrderInitialDemo")
  val deltaLog = DeltaLog.forTable(spark, new Path(tablePath))
  val snapshot = deltaLog.update()


  println(snapshot.version)  // latest version
  println(snapshot.transactions)
   // last checkpoint version

//  /home/sanskar/Project/Spark-test/spark-3.5-test/output/sample_demo/customer_zorder/customers_delta/_delta_log/00000000000000000020.checkpoint.parquet
  val checkpointPath=s"$tablePath/_delta_log/00000000000000000010.checkpoint.parquet"
  val df=readCheckpointParquet(spark,checkpointPath)
  df.printSchema()
  df.show(100,truncate=false)
}