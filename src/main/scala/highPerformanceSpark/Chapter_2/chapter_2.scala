package highPerformanceSpark.Chapter_2

import org.apache.spark.sql.{DataFrame,Dataset}
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

import org.apache.spark.sql.functions._
import highPerformanceSpark.Chapter_3._


import scala.io.StdIn



object ShuffleOperation extends App {
  val spark = SparkSession.builder()
    .appName("chapter_2_shuffle_operation")
    .master("local[*]")
    .getOrCreate()



  val rdd=spark.sparkContext.parallelize(1 to 100)


  rdd.count()
  val rdd1 = spark.sparkContext.parallelize(1 to 100, 10)
  val rdd2 = rdd1.map(x => (x % 3, x))

  val rdd3 = rdd2.groupByKey()

  println(rdd2.context.defaultParallelism)
  println(rdd3.context.defaultParallelism)


  rdd3.foreach(row=>
  println(s"Key : ${row._1} , Values : ${row._2.toList}"))

  val x=StdIn.readInt()
}



object SuffleOperationDF extends App {
  val spark = SparkSession.builder()
    .appName("chapter_2_shuffle_operation")
    .master("local[*]")
    .getOrCreate()

  val df=spark.range(1,101,4)

  val df2=df.withColumn("key",col("id")%3)
  val df3=df2.groupBy("key").agg(collect_list("id") as "values")

  df3.show()

  df.write.format("parquet").mode("overwrite").save("/home/sanskar/Project/Spark-test/spark-3.5-test/output/shuffle_example_parquet")
  val x=StdIn.readInt()
}



object DataFrameReadExample extends App {


  val spark = SparkSession.builder()
    .appName("chapter_2_rdd_reduce_example")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._


  val df = spark.read
    .option("header", "true")
    .csv("/home/sanskar/Project/Spark-test/spark-3.5-test/Data/movies")


//  df.count()

  val df3=df.sort(col("title").asc_nulls_last)
//  println(df3.explain(extended = true))
//  println(s"Distinct count :- ${df.distinct().count()}")
//
  val df2=df.dropDuplicates()


  println(df2.count())
//  println(df2.rdd.getNumPartitions)
  println(df2.explain(extended = true))
//  println(s"Distinct count after drop duplicates :- ${df2.count()}")

//
//
//  print(df.rdd.getNumPartitions)
//
//
//  df.foreachPartition {
//    partition: Iterator[Row] => {
//      println("New Partition")
////      partition.foreach(println)
//    }
//  }
//
//  print(df.queryExecution.executedPlan)
//  print(df.count())
//  val arr1 = df.take(10).toList
//  val rdd1 = spark.sparkContext.parallelize(arr1, 2)
//
//  val df2 = spark.createDataFrame(rdd1, df.schema)
//  df2.write.format("csv").option("header", "true").mode("overwrite").save("/home/sanskar/Project/Spark-test/spark-3.5-test/output/movies_output")
//  //  df.printSchema()
//  //  df.show()
//  //  print(df.rdd.getNumPartitions)
//  //  print(df.count())
//
//  val name = StdIn.readLine("Enter your name: ")
//
//
//
//
//
//
//  //
//  //
//  //
//  //  println(s"Sum of elements")
//    val s1=StdIn.readLine("Enter to exit:")
}





object RddReduceExample extends App {



  val spark=SparkSession.builder()
    .appName("chapter_2_rdd_reduce_example")
    .master("local[*]")
    .getOrCreate()

  val rdd1_temp =spark.read
    .option("header", "true")
    .csv("/home/sanskar/Project/Spark-test/spark-3.5-test/Data/movies/movies.csv")
    .rdd


//  rdd1_temp.take(10).foreach(println)

  val rdd2=rdd1_temp.filter(row =>
    row match {
      case Row(id, title, genres,_*) => true
      case _ => false
  }
  )
  println("Total Movies :- ",rdd2.count())

//  rdd2.take(10).foreach(println)

  val rdd3=rdd2.filter(row=>
    row match {
      case Row(id,title,genres:String,_*) =>
        if (genres.contains("Comedy"))  true else false
      case _ => false
    }
  )
  println("Comedy Movie :- ",rdd3.count())


  val rdd6=rdd2.filter(row=>
    row match {
      case Row(id,title,genres:String,_*) =>
        if (genres.contains("Comedy"))  false else true
      case _ => false
    }
  )
  println("Non comedy movie :- ",rdd6.count())

  val rdd4=rdd2.filter(row=>
    row match {
      case Row(id,title,genres:String,_*) => false
      case _ => true
    }
  )
  println("Invalid movies",rdd4.count())
  rdd4.take(10).foreach(row =>
    row match {
      case Row(id,title,genres,_*) =>
        println(s"Invalid Movie Record :- id : $id , title : $title , genres : $genres")
      case _ => ()
    }
  )


//
////  val rdd1=rdd1_temp.map(row=>row.get(0).toString.toInt)
////  val rdd2=rdd1.reduce((x,y)=>x+y).toInt
////
//  println(s"Sum of elements in RDD is : $rdd2")
//
//
//  println(s"Sum of elements")
}





object sparkSortExample extends App {
  val spark=SparkSession.builder()
    .appName("spark_sort_example")
    .master("local[*]")
    .getOrCreate()

  val df_temp =spark.range(1000,0,-1)
  val df2=df_temp.sort("id")
  val df3=df2.sort(col("id").desc)

  df3.show()

  df2.count()
  df2.explain("formatted")
}



object DataFrameJoinExample extends App {
  val spark = SparkSession.builder()
    .appName("chapter_2_dataframe_join_example")
    .master("local[*]")
    .getOrCreate()

  val df_stream = spark.readStream
    .option("header", "true")
    .schema( "value INT")
    .format("csv")
    .load("/home/sanskar/Project/Spark-test/spark-3.5-test/src/main/scala/highPerformanceSpark/Chapter_2/data/chapter_2/streaming_data")


  val df_stream1=df_stream.withColumn("value_squared",col("value")*col("value"))
  df_stream1.writeStream.format("console")
    .outputMode("complete")
    .start()
    .awaitTermination()





}


object DataFrame extends App {
  val spark=SparkSession.builder()
    .appName("chapter_2_dataframe")
    .master("local[*]")
    .getOrCreate()

  val df_temp =spark.range(10)
  val df2_temp=spark.range(1,1000,2)
  val df_rdd=df_temp.rdd.map(x=>Row(x*x))
  val df2_rdd=df2_temp.rdd.map(x=>Row(x*x*x))

  val df2=spark.createDataFrame(df_rdd,df_temp.schema)
  val df=spark.createDataFrame(df2_rdd,df2_temp.schema)

  val df3=df2.join(df,df("id")<=df2("id"))
    .select(df2("id").as("df2_id"),df("id").as("df_id"))
  val df4_grouped =df3.groupBy("df2_id")
  val df5=df4_grouped.agg((collect_list("df_id")) as "df_id_array")

  df5.printSchema()
  df5.explain(true)
  df5.show(10,false)
  df5.explain(true)







  val x=1

}




object Chapter2 extends App{

  def wordCount(rdd: RDD[String],words_to_elimiate: Set[String]) = {



    val wordCounts = rdd
      .flatMap(word => word.split(Array(';',',')))
      .filter(word => !words_to_elimiate.contains(word))
      .map(word => (word, 1))

    val wordCountPair = wordCounts.reduceByKey(_ + _)

    wordCountPair   // ✅ return this
  }



  def powerRDD(rdd: RDD[Int],n:Int) = {

    val poweredRDD = rdd.map(num => Math.pow(num ,n).toInt)
    poweredRDD
  }

  val spark=SparkSession.builder()
    .appName("chapter_2")
    .master("local[*]")
    .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    .config("spark.custom.config","my-name-is-sanskar")
    .getOrCreate()

//  val serializer=spark.sparkContext.getConf.get("spark.serializer")
//  println(s"Serializer in use is : $serializer")



//  val df=spark.range(10000000)
//  val rdd1=df.rdd
//  val rdd2=powerRDD(rdd1.map(x=>x.toInt),3)
//
//  println(rdd2.take(10).toList)



  val rdd1=spark.sparkContext.parallelize(List("apple,banana;orange","grape;kiwi,melon","banana;apple"))
  val rdd2=wordCount(rdd1,Set("banana","kiwi"))
  println(rdd2.collect().toList)


//  rdd1.foreach(println)


//  rdd1.foreach(
//    word => word.split("[,;]").foreach(println)
//  )
  //  val rdd1=spark.range(10)
//  rdd1.show()



//  val result=wordCount(rdd1)
//  result.foreach(println)
  val x="Extra Code to debug"










}
