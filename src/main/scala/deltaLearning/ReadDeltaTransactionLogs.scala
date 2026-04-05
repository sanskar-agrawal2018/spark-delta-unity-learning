package deltaLearning

import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, MapType, StringType, StructField, StructType}

//
// * Example utilities to inspect Delta transaction logs.
// *
// * Demonstrates:
// *  - DeltaTable.history()
// *  - Reading `_delta_log/*.json` files via Spark JSON reader
// *  - Reading a single padded-version JSON file (e.g. 00000000000000000000.json)
// *  - Reading checkpoint parquet files under `_delta_log`
// *
// * Run with: sbt "runMain deltaLearning.ReadDeltaTransactionLogs [pathToDeltaTable]"
//
object ReadDeltaTransactionLogs extends App {
  def getSparkSessionWithDelta(): SparkSession = {
    SparkSession.builder()
      .appName("ReadDeltaTransactionLogs")
      .master("local[*]")
      // Enable Delta Lake SQL extensions and catalog so Delta APIs work with Spark SQL
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
  }

  val spark = getSparkSessionWithDelta()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val defaultPath = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/nyt_covid_dv"
  val path = if (args.nonEmpty) args(0) else defaultPath

  println(s"Using Delta table path: $path")

  def showHistory(deltaPath: String): Unit = {
    try {
      import io.delta.tables.DeltaTable
      println("\n=== DeltaTable.history() ===")
      val dt = DeltaTable.forPath(spark, deltaPath)
      // history() returns a DataFrame of commits
      dt.history().show(50, truncate = false)
    } catch {
      case e: Exception => println(s"showHistory failed: ${e.getMessage}")
    }
  }

  def readAllJsonLogs(deltaPath: String): Unit = {
    val logGlob = s"${deltaPath}/_delta_log/*.json"
    println(s"\n=== Reading JSON log files: $logGlob ===")
    try {
      val df = spark.read
        .option("multiLine", false)
        .json(logGlob)
        .withColumn("file_name",input_file_name())
        .withColumn(
          "version",
          regexp_extract(col("file_name"), """.*/(\d+)\.json""", 1).cast("long")
        )
      df.printSchema()
      // show common fields found in commit JSONs


      df.orderBy(col("version")).show(truncate=false)
      df.groupBy(col("version")).agg(count(lit((1)))).orderBy(col("version")).show(truncate = false)
      val selectCols = Seq("commitInfo", "txn", "add", "remove")
      val existing = selectCols.filter(c => df.columns.contains(c))
      if (existing.nonEmpty) df.select(existing.map(df.col): _*).show(100, truncate = false)
      else df.show(50, truncate = false)
    } catch {
      case e: Exception => println(s"readAllJsonLogs failed: ${e.getMessage}")
    }




  }


  import org.apache.spark.sql.functions._

  def findActiveDeletionVectors(deltaPath: String): Unit = {

    val logPath = s"$deltaPath/_delta_log/*.json"

    // Read all JSON logs
    val df = spark.read.json(logPath)
      .withColumn("file_name", input_file_name())
      .withColumn(
        "version",
        regexp_extract(col("file_name"), """.*/(\d+)\.json""", 1).cast("long")
      )

    // Extract ADD and REMOVE actions
    val adds = df
      .filter(col("add").isNotNull)
      .select(
        col("version"),
        col("add.path").alias("path"),
        col("add.deletionVector").alias("deletionVector")
      )

    val removes = df
      .filter(col("remove").isNotNull)
      .select(
        col("version"),
        col("remove.path").alias("path")
      )

    // Get latest ADD per file (important for correctness)
    val latestAdds = adds
      .withColumn("rn", row_number().over(
        org.apache.spark.sql.expressions.Window
          .partitionBy("path")
          .orderBy(col("version").desc)
      ))
      .filter(col("rn") === 1)
      .drop("rn")

    // Get latest REMOVE per file
    val latestRemoves = removes
      .withColumn("rn", row_number().over(
        org.apache.spark.sql.expressions.Window
          .partitionBy("path")
          .orderBy(col("version").desc)
      ))
      .filter(col("rn") === 1)
      .drop("rn")

//
//    latestAdds.show(truncate=false)
//    latestRemoves.show(truncate = false)


//    latestAdds.printSchema()
//    latestAdds.filter(col("path")==="county=Hillsborough/part-00006-dbc48d56-a8b6-4d24-8c53-c7d7bdbb6961.c000.snappy.parquet").show(truncate=false)

//    latestRemoves.printSchema()
//    latest/Removes.filter(col("path")==="county=Hillsborough/part-00006-dbc48d56-a8b6-4d24-8c53-c7d7bdbb6961.c000.snappy.parquet").show(truncate=false)


    // Active files = files that are NOT removed after last add
    val activeFiles = latestAdds.alias("add")
      .join(latestRemoves.alias("remove"), latestRemoves("path")===latestAdds("path"), "left")

      .filter(
        col("remove.version").isNull ||
          col("add.version") >= col("remove.version")
      )
    println("Join completed")
    println(s"Active file ${activeFiles.count}")

//    activeFiles.filter(col("add.path")==="county=Hillsborough/part-00006-dbc48d56-a8b6-4d24-8c53-c7d7bdbb6961.c000.snappy.parquet").show()
    // Filter only files with deletion vectors
    val activeDVs = activeFiles
      .filter(col("deletionVector").isNotNull)

    println("\n=== Active Deletion Vectors ===")
    activeDVs.show(truncate = false)
  }


  def findActiveParquetFile(deltaPath: String): Unit = {

    val logPath = s"$deltaPath/_delta_log/*.json"

    // Read all JSON logs
    val df = spark.read.json(logPath)
      .withColumn("file_name", input_file_name())
      .withColumn(
        "version",
        regexp_extract(col("file_name"), """.*/(\d+)\.json""", 1).cast("long")
      )

    // Extract ADD and REMOVE actions
    val adds = df
      .filter(col("add").isNotNull)
      .select(
        col("version"),
        col("add.path").alias("path"),
        col("add.deletionVector").alias("deletionVector")
      )

    val removes = df
      .filter(col("remove").isNotNull)
      .select(
        col("version"),
        col("remove.path").alias("path")
      )

    // Get latest ADD per file (important for correctness)
    val latestAdds = adds
      .withColumn("rn", row_number().over(
        org.apache.spark.sql.expressions.Window
          .partitionBy("path")
          .orderBy(col("version").desc)
      ))
      .filter(col("rn") === 1)
      .drop("rn")

    // Get latest REMOVE per file
    val latestRemoves = removes
      .withColumn("rn", row_number().over(
        org.apache.spark.sql.expressions.Window
          .partitionBy("path")
          .orderBy(col("version").desc)
      ))
      .filter(col("rn") === 1)
      .drop("rn")

    //
    //    latestAdds.show(truncate=false)
    //    latestRemoves.show(truncate = false)


    //    latestAdds.printSchema()
    //    latestAdds.filter(col("path")==="county=Hillsborough/part-00006-dbc48d56-a8b6-4d24-8c53-c7d7bdbb6961.c000.snappy.parquet").show(truncate=false)

    //    latestRemoves.printSchema()
    //    latest/Removes.filter(col("path")==="county=Hillsborough/part-00006-dbc48d56-a8b6-4d24-8c53-c7d7bdbb6961.c000.snappy.parquet").show(truncate=false)


    // Active files = files that are NOT removed after last add
    val activeFiles = latestAdds.alias("add")
      .join(latestRemoves.alias("remove"), latestRemoves("path")===latestAdds("path"), "left")

      .filter(
        col("remove.version").isNull ||
          col("add.version") >= col("remove.version")
      )
    println("Join completed")
    println(s"Active file ${activeFiles.count}")

    //    activeFiles.filter(col("add.path")==="county=Hillsborough/part-00006-dbc48d56-a8b6-4d24-8c53-c7d7bdbb6961.c000.snappy.parquet").show()
    // Filter only files with deletion vectors
    val activeDVs = activeFiles
      .filter(col("deletionVector").isNotNull)

    println("\n=== Active Deletion Vectors ===")
    activeDVs.show(truncate = false)
  }



  def readVersionJson(deltaPath: String, version: Long): Unit = {
    // Delta versions are padded to 20 digits in filenames
    val padded = f"$version%020d"
    val file = s"${deltaPath}/_delta_log/${padded}.json"
    println(s"\n=== Reading specific version JSON file: $file ===")
    try {
      val df = spark.read.option("multiLine", true).json(file)
      df.printSchema()
      df.show(false)
    } catch {
      case e: Exception => println(s"readVersionJson failed (file may not exist): ${e.getMessage}")
    }
  }

  def readCheckpointParquet(spark:SparkSession,checkpointPath: String): DataFrame = {
//    val glob = s"${deltaPath}/_delta_log/*.checkpoint.parquet"
    println(s"\n=== Reading checkpoint parquet files (glob): $checkpointPath ===")
    try {
       spark.read.parquet(checkpointPath)

    }
      catch {
        case e: Exception =>
          throw new Exception(s"readCheckpointParquet failed: ${e.getMessage}")
      }
  }

  // Run the helpers
//  showHistory(path)
  println("Started the read all json logs")
  findActiveDeletionVectors(path)
  println("Ended  the read all json logs")
  // try reading version 0 (first commit) - will safely print an error if missing
//  readVersionJson(path, 0L)
//  readCheckpointParquet(path)

  spark.stop()
}

object CollectActiveDeltaLogMetadata extends App {
  import org.apache.hadoop.fs.{FileSystem, Path}
  import org.apache.spark.sql.{DataFrame, SparkSession}
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.delta.DeltaLog

  def getSparkSessionWithDelta(): SparkSession = {
    SparkSession.builder()
      .appName("CollectActiveDeltaLogMetadata")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
  }

  def findDeltaTableRoots(fs: FileSystem, root: Path): Seq[String] = {
    val children = Option(fs.listStatus(root)).getOrElse(Array.empty)

    val hasDeltaLog = children.exists(_.getPath.getName == "_delta_log")
    val nestedRoots = children
      .filter(_.isDirectory)
      .flatMap(status => findDeltaTableRoots(fs, status.getPath))
      .toSeq

    if (hasDeltaLog) root.toString +: nestedRoots else nestedRoots
  }

  def collectActiveFilesForTable(spark: SparkSession, tablePath: String): DataFrame = {
    val deltaLog = DeltaLog.forTable(spark, new Path(tablePath))
    val snapshot = deltaLog.update()
    val metadata = snapshot.metadata
    val protocol = snapshot.protocol

    val schema = StructType(Seq(
      StructField("numRecords", LongType, true),
      StructField("something", StringType, true),

      StructField("minValues", StructType(
        Seq(
          StructField("customer_id", IntegerType, true),
          StructField("region", StringType, true)

        )
      ), true),
      StructField("nullCount", StructType(
        Seq(
          StructField("customer_id", IntegerType, true),
          StructField("region", StringType, true)

        )
      ), true),

      StructField("maxValues", StructType(
        Seq(
          StructField("customer_id", IntegerType, true),
          StructField("region", StringType, true)

        )
      ), true)
    ))
    snapshot.allFiles.toDF()
      .withColumn("table_path", lit(tablePath))
      .withColumn("table_version", lit(snapshot.version))
      .withColumn("table_id", lit(metadata.id))
      .withColumn("table_schema", lit(metadata.schemaString))
      .withColumn("partition_columns", lit(metadata.partitionColumns.mkString(",")))
      .withColumn(
        "table_properties",
        lit(metadata.configuration.toSeq.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString(","))
      )
      .withColumn("min_reader_version", lit(protocol.minReaderVersion))
      .withColumn("min_writer_version", lit(protocol.minWriterVersion))
      .withColumn("stats_parsed", from_json(col("stats"),schema))

  }




  val spark = getSparkSessionWithDelta()
  spark.sparkContext.setLogLevel("ERROR")

  val defaultRoot = "/home/sanskar/Project/Spark-test/spark-3.5-test/output"
  val rootPath = if (args.nonEmpty) args(0) else defaultRoot
  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val tableRoots = findDeltaTableRoots(fs, new Path(rootPath)).distinct.sorted

  println(s"Scanning Delta tables under: $rootPath")
  println(s"Found ${tableRoots.size} Delta table(s)")
  tableRoots.foreach(path => println(s" - $path"))

  val activeLogMetadata = tableRoots
    .map(path => collectActiveFilesForTable(spark, path))
    .reduceOption((left, right) => left.unionByName(right, allowMissingColumns = true))

  activeLogMetadata match {
    case Some(df) =>
      val summaryColumns = Seq(
        "table_path",
        "table_version",
        "table_id",
        "path",
        "size",
        "modificationTime",
        "partitionValues",
        "stats",
        "deletionVector",
        "table_properties"
      ).filter(df.columns.contains)

      println("\n=== Active Delta file metadata across all tables ===")
      df.select(summaryColumns.map(col): _*)
        .orderBy(col("table_path"), col("path"))
        .show(200, truncate = false)

      println("\n=== Active file counts per table ===")
      df.groupBy("table_path", "table_version")
        .agg(
          count(lit(1)).alias("active_file_count"),
          sum(col("size")).alias("active_size_bytes")
        )
        .orderBy("table_path")
        .show(200, truncate = false)

    case None =>
      println("No Delta tables found under the supplied root path.")
  }

  spark.stop()
}
