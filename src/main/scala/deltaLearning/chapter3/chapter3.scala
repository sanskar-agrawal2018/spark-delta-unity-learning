package deltaLearning.chapter3
import deltaLearning.utils.operations
import io.delta.tables.DeltaTable
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.{col, e, explode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.streaming.Trigger.{AvailableNow, ProcessingTime}
import org.apache.spark.sql.{DataFrame, SparkSession}

object showDeltaHistory extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  spark.sparkContext.setLogLevel("Error")

  val deltaTable=getDeltaTable("/home/sanskar/Project/Spark-test/spark-3.5-test/output/movies_delta_2")
  deltaTable.optimize().executeCompaction()
//  spark.sql(s"ALTER TABLE delta.`/home/sanskar/Project/Spark-test/spark-3.5-test/output/movies_delta_2` SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'=true)")

  spark.sql(s"DESCRIBE EXTENDED delta.`/home/sanskar/Project/Spark-test/spark-3.5-test/output/movies_delta_2`").show(100,truncate = false)
  deltaTable.detail().show(truncate = false)
  deltaTable.history().show(truncate = false)



}


object chapter3_movie_details extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  spark.sparkContext.setLogLevel("Error")

  val deltaTable=getDeltaTable("/home/sanskar/Project/Spark-test/spark-3.5-test/output/movies_delta_2")
//  deltaTable.optimize().executeCompaction()
  //  spark.sql(s"ALTER TABLE delta.`/home/sanskar/Project/Spark-test/spark-3.5-test/output/movies_delta_2` SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'=true)")

  deltaTable.toDF.distinct().show(truncate=false)
  spark.sql(s"DESCRIBE EXTENDED delta.`/home/sanskar/Project/Spark-test/spark-3.5-test/output/movies_delta_2`").show(100,truncate = false)
  deltaTable.detail().show(truncate = false)
  deltaTable.history().show(truncate = false)

  utilsObj.stopTheThread()


}



object chapter3_movie_compaction extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  spark.sparkContext.setLogLevel("Error")

  val deltaTable=getDeltaTable("/home/sanskar/Project/Spark-test/spark-3.5-test/output/movies_delta_2")
  //  deltaTable.optimize().executeCompaction()
  //  spark.sql(s"ALTER TABLE delta.`/home/sanskar/Project/Spark-test/spark-3.5-test/output/movies_delta_2` SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'=true)")
  deltaTable.optimize().executeCompaction()
//  deltaTable.vacuum(0)

//  utilsObj.stopTheThread()



}





object chapter3_movie_vaccum extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  spark.sparkContext.setLogLevel("Error")

  val deltaTable=getDeltaTable("/home/sanskar/Project/Spark-test/spark-3.5-test/output/movies_delta_2")
//  deltaTable.optimize().executeCompaction()
  //  spark.sql(s"ALTER TABLE delta.`/home/sanskar/Project/Spark-test/spark-3.5-test/output/movies_delta_2` SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'=true)")
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
  deltaTable.vacuum(0)

  utilsObj.stopTheThread()



}

object chapter3_movie_write extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()



  //  val deltaTable=createDeltaTable(spark,"/home/sanskar/Project/Spark-test/spark-3.5-test/Data/event_json","json","/home/sanskar/Project/Spark-test/spark-3.5-test/output/events")

  spark.sparkContext.setLogLevel("Error")
  //  spark.conf.set("spark.databricks.delta.optimizeWrite","970MB")
  //  spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","true")
  //  val deltaTable=getDeltaTable("/home/sanskar/Project/Spark-test/spark-3.5-test/output/movies_delta_2")
  //
  //  deltaTable.history().show(truncate = false)

  for(i<-1 to 3) {
    println(s"Iteration :- ${i}")
    val df = spark.read.format("csv").option("header", true).load("/home/sanskar/Project/Spark-test/spark-3.5-test/Data/movies")

    df.limit(10).write.format("delta").option("optimizeWrite", "false").mode("append").save("/home/sanskar/Project/Spark-test/spark-3.5-test/output/movies_delta_2")
  }
  //  val deltaTable=getDeltaTable("/home/sanskar/Project/Spark-test/spark-3.5-test/output/events")
  //  deltaTable.detail().show()
  //  println(s"Count before :- ${deltaTable.toDF.count()}")
  //  appendDeltaTable(spark,deltaTable,"/home/sanskar/Project/Spark-test/spark-3.5-test/output/event_raw","delta")
  //  //  deltaTable.optimize().executeCompaction()
  //  println(s"Count after :- ${deltaTable.toDF.count()}")
  //  deltaTable.detail().show()
  //  deltaTable.history().show(truncate = false)


  //  deltaTable.
  //  deltaTable.
  //  spark.sql("AlTER TABLE delta.`/home/sanskar/Project/Spark-test/spark-3.5-test/output/events` ADD  COLUMN ID1 BIGINT GENERATED ALWAYS AS IDENTITY")
  //  deltaTable.toDF

  //  deltaTable.history().show(truncate = false)


  //  deltaTable.toDF.show()


  //  utilsObj.stopTheThread()




}




object chapter3_movie_dml extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()



  //  val deltaTable=createDeltaTable(spark,"/home/sanskar/Project/Spark-test/spark-3.5-test/Data/event_json","json","/home/sanskar/Project/Spark-test/spark-3.5-test/output/events")

  spark.sparkContext.setLogLevel("info")





// for(i<-1 to 3) {
//   println(s"Iteration :- ${i}")
//   val df = spark.read.format("csv").option("header", true).load("/home/sanskar/Project/Spark-test/spark-3.5-test/Data/movies")
//
//   df.limit(10).write.format("delta").option("optimizeWrite", "false").mode("append").save("/home/sanskar/Project/Spark-test/spark-3.5-test/output/movies_delta_2")
// }
  val path="/home/sanskar/Project/Spark-test/spark-3.5-test/output/movies_delta_2"

  val deltaTable1=getDeltaTable(path)

  deltaTable1.update(
    col("id")===lit("335787"),
    Map("genres"->lit("Action-Sanskar"))
  )


//  spark.sql(s"ALTER TABLE delta.`${path}` SET TBLPROPERTIES ('delta.enabledeletionVectors' = 'true')")




    utilsObj.stopTheThread()




}

object chapter3_movie_readStream extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  spark.sparkContext.setLogLevel("Error")
  val df=spark.readStream.option("ignoreChanges","true").format("delta").load("/home/sanskar/Project/Spark-test/spark-3.5-test/output/movies_delta_2")

  df.writeStream.format("delta").option("checkpointLocation","/home/sanskar/Project/Spark-test/spark-3.5-test/output/movies_delta_2_stream/_checkpoint/stream1").outputMode("append").start("/home/sanskar/Project/Spark-test/spark-3.5-test/output/movies_delta_2_stream").awaitTermination()

  utilsObj.stopTheThread()

}

object detail_read_Delta extends App with operations {

  import deltaLearning.utils.Deltautils

  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  spark.sparkContext.setLogLevel("Error")

  val df=spark.read.format("delta").load("/home/sanskar/Project/Spark-test/spark-3.5-test/output/movies_delta_2_stream")
  val df2=df.groupBy(col("id")).agg(collect_set("genres"),count(lit("1")))

  df2.printSchema()
  df2.show(truncate = false)

  utilsObj.stopTheThread()
}



object chapter3 extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()



//  val deltaTable=createDeltaTable(spark,"/home/sanskar/Project/Spark-test/spark-3.5-test/Data/event_json","json","/home/sanskar/Project/Spark-test/spark-3.5-test/output/events")

  spark.sparkContext.setLogLevel("Error")
  val deltaTable=getDeltaTable("/home/sanskar/Project/Spark-test/spark-3.5-test/output/events")
  deltaTable.detail().show()
  println(s"Count before :- ${deltaTable.toDF.count()}")
  appendDeltaTable(spark,deltaTable,"/home/sanskar/Project/Spark-test/spark-3.5-test/Data/event_json/events.json","json")
//  deltaTable.optimize().executeCompaction()
  println(s"Count after :- ${deltaTable.toDF.count()}")
  deltaTable.detail().show()
  deltaTable.history().show(truncate = false)


  //  deltaTable.
//  deltaTable.
//  spark.sql("AlTER TABLE delta.`/home/sanskar/Project/Spark-test/spark-3.5-test/output/events` ADD  COLUMN ID1 BIGINT GENERATED ALWAYS AS IDENTITY")
//  deltaTable.toDF

//  deltaTable.history().show(truncate = false)


//  deltaTable.toDF.show()


  utilsObj.stopTheThread()




}



object createUserTable extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()

  val location="/home/sanskar/Project/Spark-test/spark-3.5-test/output/user_profile_sql_table"
  spark.sql(s"""
  CREATE TABLE IF NOT EXISTS users_sql (
    userId INT,
    name STRING,
    age INT,
    city STRING,
    signup_ts STRING
  )
  USING DELTA
  LOCATION '${location}'
""")
  spark.sql(s"""
  INSERT INTO delta.`${location}` VALUES
  (101, 'Alice', 25, 'Bangalore', '2025-03-01'),
  (102, 'Bob', 30, 'Hyderabad', '2025-03-02'),
  (103, 'Charlie', 28, 'Chennai', '2025-03-03'),
  (104, 'David', 35, 'Mumbai', '2025-03-04')
""")




}


object mergeUserTable extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  val location="/home/sanskar/Project/Spark-test/spark-3.5-test/output/user_profile_sql_table"

    spark.sql(s"""
  MERGE INTO delta.`${location}` t
  USING (
    SELECT 101 AS userId, 'Z15' AS name, 31 AS age, 'Chennai' AS city, '2025-03-10' AS signup_ts
  ) s
  ON t.userId = s.userId
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
  """)


  val df=spark.read.format("delta").load(location)
  df.show(truncate=false)


  utilsObj.stopTheThread()

}


object readStreamTable extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  val location="/home/sanskar/Project/Spark-test/spark-3.5-test/output/events_stream_joined_static_user"


  val df=spark.read.format("delta").load(location)
  df.printSchema()
  df.sort(col("ingestion_time").desc).show(truncate=false)


  utilsObj.stopTheThread()

}

object readParquetTable extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()

  val df=spark.read.format("parquet").load("/home/sanskar/Project/Spark-test/spark-3.5-test/output/events_silver_parquet")



  println(df.count())
//  utilsObj.stopTheThread()

}





object deltalDelta extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  spark.sparkContext.setLogLevel("Error")
  val path="/home/sanskar/Project/Spark-test/spark-3.5-test/output/user_data_2_3"

  val deltaTable=createDeltaWithPath(spark,path)









  deltaTable.history().show(truncate = false)

  spark.sql(s"DESCRIBE EXTENDED delta.`${path}`").show(truncate = false)

  deltaTable.detail().show(truncate = false)



  deltaTable.toDF.show(truncate = false)
  utilsObj.stopTheThread()

}

object CreateDeltaUserData3 extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  val path="/home/sanskar/Project/Spark-test/spark-3.5-test/output/user_data_2"


  val deltaTable=getDeltaTable(path)



  deltaTable.toDF.coalesce(1)
    .select(min(col("detail")), max(col("detail")),count("*").alias("count"))
    .show(truncate = false)



  val path2="/home/sanskar/Project/Spark-test/spark-3.5-test/output/user_data_2"


  val deltaTable2=getDeltaTable(path)



  deltaTable2.toDF.coalesce(1)
    .select(min(col("detail")), max(col("detail")),count("*").alias("count"))
    .show(truncate = false)









  //  deltaTable.history().show(truncate = false)

  //  spark.sql(s"DESCRIBE EXTENDED delta.`${path}`").show(truncate = false)

  utilsObj.stopTheThread()

}







object AlterDelta extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  val path="/home/sanskar/Project/Spark-test/spark-3.5-test/output/user_data_2"

  val deltaTable=createDeltaWithPath(spark,path)



  spark.sql(s"ALTER TABLE delta.`${path}` CLUSTER BY (gender) ")





//  deltaTable.history().show(truncate = false)

//  spark.sql(s"DESCRIBE EXTENDED delta.`${path}`").show(truncate = false)

  utilsObj.stopTheThread()

}








object deltaMerge extends App with operations {
  import deltaLearning.utils.Deltautils


  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()



  val df1=spark.readStream.format("csv").schema("genere STRING").option("header",true).load("/home/sanskar/Project/Spark-test/spark-3.5-test/Data/movie_genere_ouput")


  def func1(batchDF:org.apache.spark.sql.DataFrame,batchId:Long): Unit ={
//    batchDF.show(truncate = false)
    val deltaTable=getDeltaTable("/home/sanskar/Project/Spark-test/spark-3.5-test/output/movie_genere_analysis")
    println(s"Batch ID: ${batchId},  Batch Count: ${batchDF.count()}")


    spark.conf.set("spark.databricks.delta.write.txnAppID", s"merge_app")
    spark.conf.set("spark.databricks.delta.write.txnVersion", "1")


    deltaTable
      .as("target")
      .merge(
        batchDF.as("source"),
        "target.genere = source.genere"
      )

      .whenMatched(col("source.genere") === "Action")
      .updateExpr(Map("count" -> "target.count + 2"))
      .whenMatched(col("source.genere").startsWith("A"))
      .updateExpr(Map("count" -> "target.count + 3"))
      .whenMatched()
      .updateExpr(Map("count" -> "target.count + 1"))
      .whenNotMatched()
      .insertExpr(Map("genere" -> "source.genere", "count" -> "1"))
      .whenNotMatchedBySource()
      .updateExpr(Map("count" -> "target.count - 1"))
      .execute()


  }

  spark.sparkContext.setLogLevel("Error")
  df1.writeStream
    .format("delta")
    .option("checkPointLocation","/home/sanskar/Project/Spark-test/spark-3.5-test/output/movie_genere_analysis/_checkpoint/movie_stream")
    .foreachBatch(func1 _)
    .trigger(ProcessingTime(1000))
//    .trigger(ProcessingTime(10000))
    .option("maxRecordPerBatch", 1)
    .start()


  utilsObj.stopTheThread()

}



object deltaCreateTable extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()

  DeltaTable
    .createIfNotExists(spark)
    .addColumn("start", "STRING")
    .addColumn("genere_aaray", "ARRAY<STRUCT<genere:STRING,count:INT>>")
    .location("/home/sanskar/Project/Spark-test/spark-3.5-test/output/movie_genere_grouped_analysis")
    .execute()

  utilsObj.stopTheThread()

}

object alterTableAddColumn extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()

  spark.sql("DESCRIBE EXTENDED delta.`/home/sanskar/Project/Spark-test/spark-3.5-test/output/movie_genere_grouped_analysis` ").show()
  spark.sql("ALTER TABLE delta.`/home/sanskar/Project/Spark-test/spark-3.5-test/output/movie_genere_grouped_analysis` ADD COLUMN id STRING")
  spark.sql("DESCRIBE EXTENDED delta.`/home/sanskar/Project/Spark-test/spark-3.5-test/output/movie_genere_grouped_analysis` ").show()


  utilsObj.stopTheThread()

}




object deltaMergeArray extends App with operations {

  import deltaLearning.utils.Deltautils

  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()

  val df1 = spark.readStream.format("csv").schema("genere STRING").option("header", true).load("/home/sanskar/Project/Spark-test/spark-3.5-test/Data/movie_genere_ouput")

  val deltaTable = getDeltaTable("/home/sanskar/Project/Spark-test/spark-3.5-test/output/movie_genere_grouped_analysis")
  def fun1(df:DataFrame,deltaDF:DataFrame): DataFrame ={

    deltaDF.show(truncate = false)
    deltaDF.printSchema()

    val deltaDF_explode=deltaDF
      .select(explode(col("genere_aaray")).alias("genere_aaray"),col("start"))
      .select(col("start"),col("genere_aaray")("genere").alias("genere"),col("genere_aaray")("count").alias("count"))

//    deltaDF_explode.printSchema()


    val df_grouped=df.groupBy(col("genere").alias("genere")).agg(count("*").alias("count"))
    df_grouped.show(truncate = false)

//    df_grouped.printSchema()




    val ans=deltaDF_explode.join(df_grouped,deltaDF_explode("genere")===df_grouped("genere"),"right")
      .select(df_grouped("genere").substr(0,1).alias("start"),df_grouped("genere"),(when(deltaDF_explode("count").isNull,0).otherwise(deltaDF_explode("count"))+df_grouped("count")).alias("new_count"))
    ans.show(truncate = false)



    deltaDF

    val transformed_df=ans
      .withColumn("start",upper(col("genere").substr(1,1)))
      .groupBy(col("start"))

      .agg(array_agg(struct(col("genere"),col("new_count").cast("INT").alias("count"))).alias("genere_aaray"))

    transformed_df.show(truncate = false)
    transformed_df.printSchema()
//    val df_grouped_1=df.groupBy(col("genere").substr(0,1).alias("start")).agg(array_agg(struct(col("genere"),count("*").alias("count"))).alias("genere_aaray"))
    return transformed_df



  }
  spark.sparkContext.setLogLevel("Error")
//  val df_write=fun1(df1,deltaTable.toDF)

//  df_write.write.format("delta").mode("append").save("/home/sanskar/Project/Spark-test/spark-3.5-test/output/movie_genere_grouped_analysis")


  def func1(batchDF: org.apache.spark.sql.DataFrame, batchId: Long): Unit = {
    println(s"Batch ID: ${batchId},  Batch Count: ${batchDF.count()}")
    batchDF.show(truncate = false)
    val deltaTable = getDeltaTable("/home/sanskar/Project/Spark-test/spark-3.5-test/output/movie_genere_grouped_analysis/")

    spark.conf.set("spark.databricks.delta.write.txnAppID", s"merge_app")
    spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
    val deltaDF=deltaTable.toDF

    val finalDF=fun1(batchDF,deltaTable.toDF)
    deltaTable.as("target")
      .merge(
        finalDF.as("source"),
        upper(col("target.start")) === upper(col("source.start"))
      )
      .whenMatched(upper(col("source.start")) === "Z")
      .updateExpr(Map("target.start" -> "target.start"))
      .whenMatched()
      .updateExpr(Map("target.genere_aaray" -> "source.genere_aaray"))

      .whenNotMatched()
      .insertExpr(Map("start" -> "source.start", "genere_aaray" -> "source.genere_aaray"))
      .execute()

  }

  

  spark.sparkContext.setLogLevel("Error")
  df1.writeStream
    .format("delta")
    .option("checkPointLocation", "/home/sanskar/Project/Spark-test/spark-3.5-test/output/movie_genere_grouped_analysis/_checkpoint/stream1")
    .foreachBatch(func1 _)
    .trigger(ProcessingTime(1000))
    .start()


  utilsObj.stopTheThread()


}


object optimizeDeltaTable extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  val path="/home/sanskar/Project/Spark-test/spark-3.5-test/output/movie_genere_grouped_analysis"
  val deltaTable=createDeltaWithPath(spark,path)
  deltaTable.optimize().executeCompaction()
//  spark.sql(s"ALTER TABLE delta.`/home/sanskar/Project/Spark-test/spark-3.5-test/output/movie_genere_grouped_analysis` SET TBLPROPERTIES ('delta.checkpoint.writeStatsAsStruct'= 'true')")
  utilsObj.stopTheThread()

}
object printDeltaArrayAgg extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  spark.sparkContext.setLogLevel("Error")
//  val df=spark.read.format("delta").load("/home/sanskar/Project/Spark-test/spark-3.5-test/output/movie_genere_grouped_analysis")
//  df.printSchema()
//  df.show(truncate = false)


  val deltaTable=DeltaTable.forPath("/home/sanskar/Project/Spark-test/spark-3.5-test/output/movie_genere_grouped_analysis")
//  deltaTable.toDF.show(truncate = false)

  deltaTable.history().show(100,truncate = false)

  deltaTable.detail().show(truncate = false)

  spark.sql(s"DESCRIBE EXTENDED  delta.`/home/sanskar/Project/Spark-test/spark-3.5-test/output/movie_genere_grouped_analysis`").show(truncate = false)




  deltaTable.toDF.show(truncate = false)
//  val df1 = spark.read.format("csv").schema("genere STRING").load("/home/sanskar/Project/Spark-test/spark-3.5-test/Data/movie_genere")
//  df1.show(truncate = false)
  utilsObj.stopTheThread()

}


object deltaTableDetailMovieGenre  extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()

//  DeltaTable
//    .createOrReplace(spark)
//    .addColumn("genere", "STRING")
//    .addColumn("count", "INT")
//    .location("/home/sanskar/Project/Spark-test/spark-3.5-test/output/movie_genere_analysis")
//    .execute()
  spark.sparkContext.setLogLevel("Error")
  val path="/home/sanskar/Project/Spark-test/spark-3.5-test/output/movie_genere_analysis"
  val deltaTable=getDeltaTable(path)


//  deltaTable.delete()





  //  deltaTable.updateExpr("id='96'", Map("gender" -> "'F'"))

  deltaTable.toDF.show(truncate = false)
  //  deltaTable.optimize().executeCompaction()



  deltaTable.detail().show(truncate = false)
  deltaTable.history().show(truncate = false)
  //  spark.sql(s"ALTER TABLE delta.`${location}` ADD COLUMN id_sanskar STRING")
  //  val df1=deltaTable.toDF.select("firstName","gender","dateOfBirth").filter(col("id")===16)
  //  df1.write.format("delta").mode("append").save(location)
  //  deltaTable.toDF.filter(col("firstName")==="John").show(truncate = false)
  utilsObj.stopTheThread()

}



object  deltatTableMerge extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  val path="/home/sanskar/Project/Spark-test/spark-3.5-test/output/user_data_2"
  val deltaTable=createDeltaWithPath(spark,path)


}




object deltaTableDetail extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  spark.sparkContext.setLogLevel("Error")
  val path="/home/sanskar/Project/Spark-test/spark-3.5-test/output/user_data_2"
  val deltaTable=createDeltaWithPath(spark,path)


  val location=deltaTable.detail().select(col("location")).collect()(0).getString(0)
  println(location==path)



//  deltaTable.updateExpr("id='96'", Map("gender" -> "'F'"))

  deltaTable.toDF.show(truncate = false)
//  deltaTable.optimize().executeCompaction()



  deltaTable.detail().show(truncate = false)
  deltaTable.history().show(truncate = false)
//  spark.sql(s"ALTER TABLE delta.`${location}` ADD COLUMN id_sanskar STRING")
//  val df1=deltaTable.toDF.select("firstName","gender","dateOfBirth").filter(col("id")===16)
//  df1.write.format("delta").mode("append").save(location)
//  deltaTable.toDF.filter(col("firstName")==="John").show(truncate = false)
  utilsObj.stopTheThread()

}



object deltaTableDML extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  val path="/home/sanskar/Project/Spark-test/spark-3.5-test/output/user_data_2"
  val deltaTable=createDeltaWithPath(spark,path)


  val location=deltaTable.detail().select(col("location")).collect()(0).getString(0)


//  spark.sql("REORG TABLE delta.`/home/sanskar/Project/Spark-test/spark-3.5-test/output/user_data_2` APPLY(PURGE)")

    deltaTable.updateExpr("id='100'", Map("gender" -> "'F'"))

  deltaTable.toDF.show(truncate = false)
  //  deltaTable.optimize().executeCompaction()



  deltaTable.detail().show(truncate = false)
  deltaTable.history().show(truncate = false)
  //  spark.sql(s"ALTER TABLE delta.`${location}` ADD COLUMN id_sanskar STRING")
  //  val df1=deltaTable.toDF.select("firstName","gender","dateOfBirth").filter(col("id")===16)
  //  df1.write.format("delta").mode("append").save(location)
  //  deltaTable.toDF.filter(col("firstName")==="John").show(truncate = false)
  utilsObj.stopTheThread()

}






object deltaTableAlter extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  val path="/home/sanskar/Project/Spark-test/spark-3.5-test/output/user_data_2"
  val deltaTable=createDeltaWithPath(spark,path)


  val location=deltaTable.detail().select(col("location")).collect()(0).getString(0)


//    spark.sql(s"Alter table delta.`${location}` UNSET TBLPROPERTIES ('delta.delectionVectors.enabled')")

//  deltaTable.updateExpr("id='46'", Map("gender" -> "'F'"))

  deltaTable.toDF.show(truncate = false)
    deltaTable.optimize().executeCompaction()


  deltaTable.detail().show(truncate = false)
  deltaTable.history().show(truncate = false)
  //  spark.sql(s"ALTER TABLE delta.`${location}` ADD COLUMN id_sanskar STRING")
  //  val df1=deltaTable.toDF.select("firstName","gender","dateOfBirth").filter(col("id")===16)
  //  df1.write.format("delta").mode("append").save(location)
  //  deltaTable.toDF.filter(col("firstName")==="John").show(truncate = false)
  utilsObj.stopTheThread()

}





object testIdentityColumn extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  val path="/home/sanskar/Project/Spark-test/spark-3.5-test/output/user_data_2"
  val deltaTable=createDeltaWithPath(spark,path)


  val data=Seq(
    ("John", "M", "1990-05-10 10:15:00",1),
    ("Jane", "F", "1985-07-21 08:30:00",2),
    ("Mike", "M", "1992-12-01 14:00:00",3)
  )
  val schema=StructType(
    Seq(
      StructField("firstName",StringType,true),
      StructField("gender",StringType,true),
      StructField("dateOfBirth",StringType,true),
      StructField("fullName",StringType,true)

    )
  )
  import org.apache.spark.sql.Row
  val rowData = data.map {
    case (firstName, gender, birthDate, id) =>
      Row(firstName, gender, birthDate,s"${firstName}_${gender}")
  }
  val df=spark.createDataFrame(spark.sparkContext.parallelize(rowData),schema).withColumn("dateOfBirth",col("dateOfBirth").cast(DateType))
  df.printSchema()

  println(s"Before count :-  ${deltaTable.toDF.count()}")

  df.write.format("delta").option("txnVersion",1).option("txnAppId","sanskar").mode("append").save(path)


  println(s"After count :- ${deltaTable.toDF.count()}")
  deltaTable.toDF.show(truncate = false)

  //  deltaTable.detail().show(truncate = false)
  utilsObj.stopTheThread()

}

object printDeltaTable extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  val path="/home/sanskar/Project/Spark-test/spark-3.5-test/output/user_data_2"

  val deltaTable=getDeltaTable("/home/sanskar/Project/Spark-test/spark-3.5-test/output/user_data_2")

  detailDeltaTable(spark,deltaTable)


//  deltaTable.toDF.show(5,false)

  utilsObj.stopTheThread()

}




object insertData extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()

  val deltaTable=getDeltaTable("/home/sanskar/Project/Spark-test/spark-3.5-test/output/events")

//  println(deltaTable.toDF.count())
  val df1=deltaTable.toDF

//  val df2=spark.range(10).toDF("id")
  df1.printSchema()

  val df1_transformed=df1.withColumn("transaction", explode(col("transactions")))
    .select(col("eventId"),col("success"),col("transaction")("amount").alias("amount")).distinct()

//  df1_transformed.show(5,false)
//  df1.show(5,false)
//  val df=df1.filter("success=true").limit(2)

    val df=df1.join(df1_transformed,"eventId","inner").select(df1("*"),df1_transformed("amount"))
    df.printSchema()



  deltaTable.as("events")
    .merge(
      df.as("new_data"),
      "1==0"
    )
    .whenMatched()
    .updateAll()
    .whenNotMatched()
    .insertAll()
    .execute()


  deltaTable.toDF.explain(extended = true)
  println("After Merge:")
//  println(deltaTable.toDF.count())

  utilsObj.stopTheThread()

}

//object createTableWithName extends App  with operations {
//  import deltaLearning.utils.Deltautils
//  val utilsObj = new Deltautils()
//  val spark = utilsObj.getSparkSession()
//
////  createDeltaWithName(spark)
//
//  val data = Seq(
//    ("John", "A", "1990-05-10 10:15:00", "M", "123-45-6789", 80000),
//    ("Jane", "B", "1985-07-21 08:30:00", "F", "987-65-4321", 95000),
//    ("Mike", null, "1992-12-01 14:00:00", "M", "456-78-9012", 72000)
//  )
//
//  val df_data=spark.createDataFrame(data).toDF("firstName", "middleName", "birthDate","gender", "ssn", "salary").withColumn("birthDate",col("birthDate").cast("timestamp"))
//
//
//
//  df_data.write.format("delta").mode("append").saveAsTable("default.people1")
//
//  val df=DeltaTable.forName("default.people1").toDF
//
//  df.printSchema()
//
//  df.show(5,false)
//
//
//
//
//  utilsObj.stopTheThread()
//}
//









import org.apache.spark.sql.SparkSession

object DeltaLakeLagCalculator {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .appName("DeltaLakeStreamLag")
      .getOrCreate()

    val states = Map(
      "App1"
        -> "s3://<CheckPoint_location_App1>/_delta_log/",
      "App2"
        -> "s3://<CheckPoint_location_2>/_delta_log/"
    )

    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    for (pair <- states) {

      val application = pair._1
      val deltaLogLocation = pair._2

      val log_output_path = s"/mnt/var/log/hadoop-yarn/containers/application_lags/container_lags/" +
        s"${application}/stdout"
      val offsetFileList =
        sparkSession
          .read.format("json")
          .load(s"s3://<application_path_to_stream>/" +
            s"${application}/v1/_checkpoint/offsets/")
          .select("reservoirVersion")
          .filter("reservoirVersion is not null")
          .withColumn(
            "path", concat(lit(deltaLogLocation),
              lpad($"reservoirVersion",
                20, "0"), lit(".json")))
          .select("path")
          .collect()
          .map(_ (0).toString).toList

      val currentOffsets =
        sparkSession
          .read
          .json(offsetFileList: _*)
          .select("commitInfo.timestamp")
          .filter("commitInfo.timestamp is not null")
          .distinct().withColumn("current_timestamp", unix_timestamp() * 1000)

      val diff_secs_col = col("current_timestamp").cast("long") -
        col("timestamp").cast("long")
      val lag_from_current_time = currentOffsets
        .withColumn("diff_hours", diff_secs_col / 3600000)
        .select("diff_hours")

      val list = lag_from_current_time.agg(max($"diff_hours") as "total_lag").collect()

      list.foreach(element => println("lag=" + element, " Application=" + "application"))

    }
  }

}




object cdcCreateAndInsert extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  spark.sparkContext.setLogLevel("ERROR")

  val path = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/cdc_demo_1"

  // Create table with Change Data Feed enabled
  spark.sql(s"""
    CREATE TABLE IF NOT EXISTS delta.`${path}` (
      id INT,
      name STRING,
      age INT
    )
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true')
    LOCATION '${path}'
  """)

  // Insert initial rows
  spark.sql(s"INSERT INTO delta.`${path}` VALUES (1, 'Alice', 30), (2, 'Bob', 25)")

  println("After initial insert:")
  spark.read.format("delta").load(path).show(false)

  utilsObj.stopTheThread()
}

object cdcInsertRecords extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  spark.sparkContext.setLogLevel("ERROR")

  val path = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/cdc_demo_1"

  // Update a record
  val df=spark.read.format("delta").load(path)
  df.coalesce(1).write.format("delta").mode("append").save(path)


  println("After update:")
  spark.read.format("delta").load(path).show(false)

  utilsObj.stopTheThread()
}




object cdcUpdateRecords extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  spark.sparkContext.setLogLevel("ERROR")

  val path = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/cdc_demo_1"

  // Update a record
  spark.sql(s"UPDATE delta.`${path}` SET age = 14 WHERE id = 1")

  println("After update:")
  spark.read.format("delta").load(path).show(false)

  utilsObj.stopTheThread()
}


object cdcDeleteRecords extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  spark.sparkContext.setLogLevel("ERROR")

  val path = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/cdc_demo_1"

  // Delete a record and add another to generate more change events
  spark.sql(s"DELETE FROM delta.`${path}` WHERE id = 2")
  spark.sql(s"INSERT INTO delta.`${path}` VALUES (3, 'Charlie', 40)")

  println("After delete and another insert:")
  spark.read.format("delta").load(path).show(false)

  utilsObj.stopTheThread()
}


object cdcReadChangeFeedBatch extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  spark.sparkContext.setLogLevel("ERROR")

  val path = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/cdc_demo_1"

  // Read change data feed in batch from the beginning
  // startingVersion can be set to a specific version if desired
  val cdf = spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", 0).load(path)

  println("Change Data Feed (batch, startingVersion=0):")
  cdf
    .select("_change_type", "id", "name", "age", "_commit_version", "_commit_timestamp")
    .orderBy("_commit_version", "id")
    .show(false)

  // You can also read only a version range if you like
  // val cdfRange = spark.read.format("delta").option("readChangeFeed","true").option("startingVersion",1).option("endingVersion",3).load(path)

  utilsObj.stopTheThread()
}




object readChangeDeltaParuqet extends App with operations {
  import deltaLearning.utils.Deltautils
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  spark.sparkContext.setLogLevel("ERROR")

  val path = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/cdc_demo_1/_delta_log/00000000000000000010.checkpoint.parquet"

  // Read change data feed in batch from the beginning
  // startingVersion can be set to a specific version if desired
  val cdf = spark.read.format("parquet").load(path)

  println("Change Data Feed (batch, startingVersion=0):")
  cdf.show(100,false)


//  spark.read.format("parquet").load("/home/sanskar/Project/Spark-test/spark-3.5-test/output/cdc_demo_1/part-00000-7868e131-0f6e-41c3-9255-e1c85469362d.c000.snappy.parquet").show(truncate=false)

  println("Change Data Feed (batch, startingVersion=0):")


  utilsObj.stopTheThread()
}

object cdcStreamReaderWithInitialPosition extends App with operations {
  import deltaLearning.utils.Deltautils
  import org.apache.spark.sql.streaming.Trigger.AvailableNow
  val utilsObj = new Deltautils()
  val spark = utilsObj.getSparkSession()
  spark.sparkContext.setLogLevel("ERROR")

  val path = "/home/sanskar/Project/Spark-test/spark-3.5-test/output/cdc_demo_1"

  // Stream read from change data feed starting at version 0 and write to console once
  val streamDF = spark.readStream.format("delta").option("readChangeFeed", "true").option("startingVersion", 0).load(path)

  val query = streamDF.writeStream
    .format("console")
    .option("truncate", false)
    .trigger(AvailableNow())
    .start()

  // Wait until the one-time available-now trigger completes
  query.awaitTermination()

  utilsObj.stopTheThread()
}

// End of appended CDC demo objects
