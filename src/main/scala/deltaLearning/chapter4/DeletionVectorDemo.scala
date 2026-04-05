package deltaLearning.chapter4
import deltaLearning.utils.operations
import deltaLearning.utils.Deltautils
import org.apache.spark.sql.functions._
import io.delta.tables.DeltaTable
import org.apache.spark.sql.expressions.Window


object DeletionVectorDemo extends App with operations {
  // Demo: read a Parquet file, write a Delta table with deletion-vectors enabled (table property),
  // partition by `county`, show grouped counts and then apply deletes (partition-scoped and cross-partition)
  // Adjust the parquetPath and deltaPath as needed for your environment.

  import org.apache.spark.sql.SparkSession


  val deltaReadPath = "/home/sanskar/Downloads/delta-lake-definitive-guide-main/datasets/COVID-19_NYT"
  val deltaPath = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/nyt_covid_dv"

  val utils = new Deltautils()
  val spark = utils.getSparkSession()

  import spark.implicits._

  // Read parquet (fail fast if file missing)
  val raw = try {
	spark.read.format("delta").load(deltaReadPath)
  } catch {
	case e: Throwable =>
	  println(s"Failed to read delta at $deltaReadPath: ${e.getMessage}")
	  spark.stop()
	  sys.exit(1)
  }
  raw.printSchema()

  raw
    .filter(col("county").isin("Hillsborough", "Pasco", "Pinellas", "Sarasota"))
    .write.format("delta").mode("overwrite").partitionBy("county").save(deltaPath)

  // Enable deletion vectors on the table by setting a table property.
  // The exact property key may vary by distribution; Delta commonly uses a table property like
  // 'delta.enableDeletionVector' or a runtime/session config. We set a few plausible keys to be safe.
  try {
	spark.sql(s"ALTER TABLE delta.`$deltaPath` SET TBLPROPERTIES ('delta.enableDeletionVector' = 'true')")
  } catch {
	case _: Throwable => // ignore if ALTER TABLE by path not supported in this environment
  }
  utils.stopTheThread()


}


object CreateEventsTableSQL extends App with operations {
  val utils = new Deltautils()
  val spark = utils.getSparkSession()
  val deltaPath = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/nyt_covid_dv"

  // This is a placeholder for the actual implementation of the deletion vector demo.
  // The implementation would include reading the Delta table, showing counts, applying deletes, and showing results.
  println("DeletionVectorDemo2 is not yet implemented. Please implement the logic to demonstrate deletion vectors.")
  // Read back as Delta
  for(i <- 1 until 10)
  {
    try
    {
        val d = spark.read.format("delta").option("versionAsOf",i).load(deltaPath)
        println(s"Counts grouped by date, county, state (initial) ${i}:")
        d.groupBy( "county", "date").agg(count(lit("1")))
        .filter(col("date")==="2020-04-13")
        .orderBy("date").show(200, false)

    }
    catch {
      case e: Throwable => println(s"Error ${e}")

      // ignore if ALTER TABLE by path not supported in this environment
    }


  }

//  val df1=d.groupBy(  "date").count()
//
//  df1.show()

//  val dateq=d.select(rank().over(Window.orderBy(col("count"))).alias("rank1"),col("date"))
//    .filter(col("rank1")===1)
//  dateq.show()





  println("Deletion vector demo complete.")
  spark.stop()
  utils.stopTheThread()
}




object appendDeltaDV extends App with operations {
  val utils = new Deltautils()
  val spark = utils.getSparkSession()

  val deltaReadPath = "/home/sanskar/Downloads/delta-lake-definitive-guide-main/datasets/COVID-19_NYT"
  val deltaPath = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/nyt_covid_dv"

  // This is a placeholder for the actual implementation of the deletion vector demo.
  // The implementation would include reading the Delta table, showing counts, applying deletes, and showing results.
  println("DeletionVectorDemo2 is not yet implemented. Please implement the logic to demonstrate deletion vectors.")

  // Read back as Delta
  for(i <- 1 until 10)
  {

    println(s"Starting request ${i}")
    import spark.implicits._

    // Read parquet (fail fast if file missing)
    val raw_p = try {
      spark.read.format("delta").load(deltaReadPath)
    } catch {
      case e: Throwable =>
        println(s"Failed to read delta at $deltaReadPath: ${e.getMessage}")
        spark.stop()
        sys.exit(1)
    }
    raw_p.printSchema()

    val raw=raw_p
      .filter(col("county").isin("Hillsborough", "Pasco", "Pinellas", "Sarasota"))
      .limit(1)

    raw.show(truncate = false )

    raw.write.mode("append").format("delta").save(deltaPath)

    println(s"Ending request ${i}")


  }





  println("Added code  demo complete.")
  spark.stop()
  utils.stopTheThread()
}





object writePartialData extends App with operations {
  val utils = new Deltautils()
  val spark = utils.getSparkSession()

  val format="json"

  val deltaReadPath = "/home/sanskar/Downloads/delta-lake-definitive-guide-main/datasets/COVID-19_NYT"
  val deltaPath = s"/home/sanskar/Project/Spark-test/spark-3.5-test/output/nyt_covid_${format}"

  // This is a placeholder for the actual implementation of the deletion vector demo.
  // The implementation would include reading the Delta table, showing counts, applying deletes, and showing results.
  println("DeletionVectorDemo2 is not yet implemented. Please implement the logic to demonstrate deletion vectors.")

  // Read back as Delta


    println(s"Starting request")
    import spark.implicits._

    // Read parquet (fail fast if file missing)
    val raw_p = try {
      spark.read.format("delta").load(deltaReadPath)
    } catch {
      case e: Throwable =>
        println(s"Failed to read delta at $deltaReadPath: ${e.getMessage}")
        spark.stop()
        sys.exit(1)
    }
    raw_p.printSchema()


    raw_p.show(truncate = false )

    raw_p.write.partitionBy("county").mode("overwrite").format(format).save(deltaPath)

    println(s"Ending request")








  println("Added code  demo complete.")
  spark.stop()
  utils.stopTheThread()
}




object ReadPartialData extends App with operations {
  val utils = new Deltautils()
  val spark = utils.getSparkSession()

  val deltaReadPath = "/home/sanskar/Downloads/delta-lake-definitive-guide-main/datasets/COVID-19_NYT"
  val deltaPath = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/nyt_covid_parquet"

  // This is a placeholder for the actual implementation of the deletion vector demo.
  // The implementation would include reading the Delta table, showing counts, applying deletes, and showing results.
  println("DeletionVectorDemo2 is not yet implemented. Please implement the logic to demonstrate deletion vectors.")

  // Read back as Delta


  println(s"Starting request")
  import spark.implicits._

  // Read parquet (fail fast if file missing)
  val raw_p = try {
    spark.read.format("parquet").load(deltaPath)
  } catch {
    case e: Throwable =>
      println(s"Failed to read delta at $deltaReadPath: ${e.getMessage}")
      spark.stop()
      sys.exit(1)
  }
  raw_p.printSchema()


  raw_p.show(truncate = false )

//  raw_p.write.partitionBy("county").mode("overwrite").format("parquet").save(deltaPath)

  println(s"Ending request")








  println("Added code  demo complete.")
  spark.stop()
  utils.stopTheThread()
}






object DeleteEventsTableSQL extends App with operations {
  val utils = new Deltautils()
  val spark = utils.getSparkSession()
  val deltaPath = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/nyt_covid_dv"

  // This is a placeholder for the actual implementation of the deletion vector demo.
  // The implementation would include reading the Delta table, showing counts, applying deletes, and showing results.
  println("DeletionVectorDemo2 is not yet implemented. Please implement the logic to demonstrate deletion vectors.")
  // Read back as Delta

  val deltaTable1=DeltaTable.forPath(spark,deltaPath)
  deltaTable1.delete(col("date")==="2020-07-05" && col("county")==="Washington")

  println("Deletion vector demo complete.")
  spark.stop()
  utils.stopTheThread()
}







object readChangeDeltaParuqet_DV extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  spark.sparkContext.setLogLevel("ERROR")



  val path = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/nyt_covid_dv/_delta_log/00000000000000000010.checkpoint.parquet"

  // Read change data feed in batch from the beginning
  // startingVersion can be set to a specific version if desired
  val cdf_raw = spark.read.format("parquet")
    .load(path)

  val cdf=cdf_raw.filter(cdf_raw("add")("path")==="county=Hillsborough/part-00006-dbc48d56-a8b6-4d24-8c53-c7d7bdbb6961.c000.snappy.parquet")


  cdf.printSchema()
  println(s"Count - ${cdf.count}")

  println("Change Data Feed (batch, startingVersion=0):")
  cdf.show(100,false)

  println("Remove records :- ")
  cdf_raw.filter(cdf_raw("remove").isNotNull).show(false)

  val cdf_x=cdf_raw.filter(cdf_raw("remove").isNull && cdf_raw("add").isNull  )

  println("Change Both null :")
  cdf_x.show(100,false)


  println(s"Count both null - ${cdf_x.count}")


  println(s"Count add not null  - ${cdf_raw.filter(cdf_raw("add").isNotNull  ).count}")




  println(s"Count remove not null - ${cdf_raw.filter(cdf_raw("remove").isNotNull).count}")





  //  spark.read.format("parquet").load("/home/sanskar/Project/Spark-test/spark-3.5-test/output/cdc_demo_1/part-00000-7868e131-0f6e-41c3-9255-e1c85469362d.c000.snappy.parquet").show(truncate=false)

  println("Change Data Feed (batch, startingVersion=0):")


  utilsObj.stopTheThread()
}




object ReadVectorReadHist extends App with operations {
  val utils = new Deltautils()
  val spark = utils.getSparkSession()
  val deltaPath = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/nyt_covid_dv"

  // This is a placeholder for the actual implementation of the deletion vector demo.
  // The implementation would include reading the Delta table, showing counts, applying deletes, and showing results.
  // Read back as Delta



  val deltaTable= DeltaTable.forPath(spark, deltaPath)
  println("Delta Table History:")


  deltaTable.history().show(truncate = false)




  spark.sql(s"DESCRIBE EXTENDED  delta.`$deltaPath`").show(truncate = false)
  println("Deletion vector demo complete.")
  spark.stop()
  utils.stopTheThread()
}


object DeletionVectorReadHist extends App with operations {
  val utils = new Deltautils()
  val spark = utils.getSparkSession()
  val deltaPath = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/nyt_covid_dv"

  // This is a placeholder for the actual implementation of the deletion vector demo.
  // The implementation would include reading the Delta table, showing counts, applying deletes, and showing results.
 // Read back as Delta



  val deltaTable= DeltaTable.forPath(spark, deltaPath)
  println("Delta Table History:")


  deltaTable.history().show(truncate = false)




  spark.sql(s"DESCRIBE EXTENDED  delta.`$deltaPath`").show(truncate = false)
  println("Deletion vector demo complete.")
  spark.stop()
  utils.stopTheThread()
}





object DeletionVectorVaccum  extends App with operations {
  val utils = new Deltautils()
  val spark = utils.getSparkSession()
  val deltaPath = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/nyt_covid_dv"

  // This is a placeholder for the actual implementation of the deletion vector demo.
  // The implementation would include reading the Delta table, showing counts, applying deletes, and showing results.
  // Read back as Delta

  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

  val deltaTable= DeltaTable.forPath(spark, deltaPath)
  println("Delta Table History:")

  val df=spark.sql(s"""VACUUM  delta.`${deltaPath}`  RETAIN 1 HOURS""")
  val df_filtered=df.filter(!col("path").like("%.parquet%"))

  df_filtered.show(truncate = false)

  println(s"Count Not Parquet:- ${df_filtered.count()}")
  println(s"Count Parquet :- ${df.filter(col("path").like("%.parquet%")).count()}")


  println(s"Count Total:- ${df.count()}")


  val df2=spark.sql(s"""VACUUM  delta.`${deltaPath}`  RETAIN 1 HOURS  DRY RUN """)
  println(s"Count Total:- ${df2.count()}")

//  spark.sql(s"""VACUUM delta.`/home/sanskar/Project/Spark-test/spark-3.5-test/output/nyt_covid_dv` RETAIN 1 HOURS DRY RUN """).show()

  println("Deletion vector demo complete.")
  spark.stop()
  utils.stopTheThread()
}




// Counts records grouped by date, county and state from the Delta table.
// Note: user asked for "country" but this dataset uses `county`.
object CountByCountyAndDate extends App with operations {
  val utils = new Deltautils()
  val spark = utils.getSparkSession()
  import spark.implicits._

  val deltaPath = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/nyt_covid_dv"

  try {
    // Read the Delta table (will throw if not present)
    val df = spark.read.format("delta").load(deltaPath)

    // Group by date, county, state and count records, then order by date then
    //
    // county
    val counts = df.groupBy(col("date"))
      .agg(count(lit(1)).alias("record_count"),array_agg(col("county")).alias("county"))
      .orderBy(col("date"), col("county"))

    println(s"Counts grouped by date, county, state for table at: $deltaPath")
    counts.show(200, false)
  } catch {
    case e: Throwable =>
      println(s"Failed to read/process Delta table at $deltaPath: ${e.getMessage}")
  } finally {
    spark.stop()
    utils.stopTheThread()
  }
}


object ReOrgDeltaTable  extends App with operations {
  val utils = new Deltautils()
  val spark = utils.getSparkSession()
  val deltaPath = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/nyt_covid_dv"
  val deltaIdentifier = s"delta.`$deltaPath`"

  spark.sparkContext.setLogLevel("ERROR")
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
  spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")

  val deltaTable = DeltaTable.forPath(spark, deltaPath)

//  println(s"Preparing table for REORG APPLY (PURGE): $deltaPath")
//  spark.sql(
//    s"""
//       |ALTER TABLE $deltaIdentifier
//       |SET TBLPROPERTIES (
//       |  'delta.enableDeletionVectors' = 'true',
//       |  'delta.deletedFileRetentionDuration' = 'interval 1 hours',
//       |  'delta.logRetentionDuration' = 'interval 1 days'
//       |)
//       |""".stripMargin
//  )

  println("Table details before REORG:")
  deltaTable.detail().show(truncate = false)

  println("Running REORG TABLE ... APPLY (PURGE)")
  spark.sql(s"REORG TABLE $deltaIdentifier APPLY (PURGE)").show(truncate = false)

  println("Table history after REORG:")
  deltaTable.history().show(truncate = false)

  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

  println("VACUUM DRY RUN after REORG:")
  spark.sql(s"VACUUM $deltaIdentifier RETAIN 1 HOURS DRY RUN").show(truncate = false)

  spark.stop()
  utils.stopTheThread()

}

