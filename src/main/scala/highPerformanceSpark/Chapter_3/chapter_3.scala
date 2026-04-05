package highPerformanceSpark.Chapter_3

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructField, StructType, TimestampType}
import highPerformanceSpark.utils.utils
import org.apache.spark.sql.expressions.Window
import org.apache.parquet.format.TimeType

import org.apache.spark.sql.catalyst.expressions._



object RDDGroupByExample extends App {
  val spark = SparkSession.builder()
    .appName("chapter_3_rdd_groupbykey_example")
    .master("local[*]")
    .getOrCreate()

  println(spark.version)
  val df2=spark.range(100)

  spark.sparkContext.setLogLevel("DEBUG")

  println(1.to(10))
  df2.createTempView("numbers")

//  val a:AnyRef = "Sanskar"
//RDDGroupByExample
  spark.sql("select * from numbers").show()
//  print(a.eq("Sanskar"))

//  val df = spark.read
//    .option("header", "true")
//    .csv("/home/sanskar/Project/Spark-test/spark-3.5-test/Data/movies")

  case class Panda(a:Int,b:String,c:Double)
  case class Animal(name:String,age:Int,panda:Panda)

  val data = Seq(
    Animal("Lion", 5, Panda(1,"Giant", 250.5)),
    Animal("Tiger", 3, Panda(2,"Red", 150.0)),
    Animal("Bear", 7, Panda(2,null, 2)),
    Animal("Wolf", 4, null)
  )




  val df=spark.createDataFrame(data)
  val df_unique=df.dropDuplicates()
  df_unique.explain(true)
  df.withColumn("is_NULL_pandas",isnull(col("panda"))).select(col("panda.a"),col("panda.b")).show()



//  df.printSchema()

}


object GroupByExample extends App with utils {
  val spark= sparkSessionInit()
  val df=spark.read.option("header","true").csv("/home/sanskar/Project/Spark-test/spark-3.5-test/Data/movies/movies.csv")
  df.printSchema()

  val expr=Add(Literal(6),Literal(7))
  println(expr.eval())

  val df2=df.withColumn("generes_split", split(col("genres"), "[\\|\\-,\\s]+"))
    .withColumn("generes_exploded", explode(col("generes_split")))
    .groupBy("generes_exploded")
    .agg(Map("title" -> "count", "budget" -> "avg"))
//    .show(100,false)


//  df2.explain(extended=true )



  df.printSchema()

  val df3=df.select(
    unhex(
      sha2(concat_ws("|", col("genres"),col("id")), 256)
    ).alias("Binary_primary_key"),
    sha2(concat_ws("|",col("genres"),col("id")),256).alias("primary_key") , hash(col("genres"),col("id")).alias("hash1"),md5(col("id")).alias("hash2_md5"),xxhash64(col("genres"),col("id")).alias("hashxx"),hash(col("genres"),col("id")).alias("hash_Gener"),col("genres")).filter(col("genres").contains(" I'm a Faggot Nigger Jew"))

  df3.show(truncate=false)
  spark.sparkContext.setLogLevel("ERROR")

  df3.write.format("delta").mode("overwrite").save("/home/sanskar/Project/Spark-test/spark-3.5-test/output/movies_output")








  val x=123

}



object writeFile extends App with utils {
  val spark= sparkSessionInit()
  spark.sparkContext.setLogLevel("ERROR")
    val schema = StructType(
      Seq(
        StructField("genere", StringType)
      )
    )
    val df = spark.read.format("csv").schema(schema).load("/home/sanskar/Project/Spark-test/spark-3.5-test/Data/movie_genere")
//    df.show(truncate = false)
    df.write.format("csv").mode("append").option("header", true).save("/home/sanskar/Project/Spark-test/spark-3.5-test/Data/movie_genere_ouput")
    println(s"Count :- ${spark.read.format("csv").load("/home/sanskar/Project/Spark-test/spark-3.5-test/Data/movie_genere_ouput").count()}")


  stopTheThread()
}


object ReadMoviesDelta  extends App with utils {
  val spark= sparkSessionInit()
  val df3=spark.read.format("delta").load("/home/sanskar/Project/Spark-test/spark-3.5-test/output/movies_output")
  spark.sparkContext.setLogLevel("ERROR")
  df3.show(truncate = false)
  df3.printSchema()







  val x=123

}








object WindowFunction extends App with utils {
  val spark=sparkSessionInit()
  val df=cleanComplexUserDataFrame(spark)

//  df.printSchema()
  val df2=df.withColumn("product_purchased",explode(col("products")))


  val windowSpec=Window.partitionBy(col("product_purchased")("id"))

  df2.show(truncate=false)
  df2.explain(true)
  val df3=df2.select(col("id"),col("product_purchased")("id"),count(lit(1)).over(windowSpec),collect_list(col("product_purchased")("name")).over(windowSpec))
//  val df3=df2.select(collect_set(col("product_purchased")).alias("set"),collect_list(col("product_purchased")).alias("list"))


  df3.show(truncate=false)
  df3.explain(extended = true)

  stopTheThread()



//  df.show()
}

object ArrayAggreate extends App with utils  {
  val spark=sparkSessionInit()

  def udfAdd(seq_x: Seq[Int]): Seq[Int]={
    seq_x.map(value => value+1)
  }
  val plusOneInt=spark.udf.register("plusOneInt",udfAdd(_:Seq[Int]):Seq[Int])


  case class Pandas(id:Int,attributes:Seq[Int])
  val data=Seq(
    Pandas(1,Seq(1,2,4)),
    Pandas(2,Seq(2,45,6)),
    Pandas(3,Seq(2,45,6)),
    Pandas(4,Seq())
  )
  val df=spark.createDataFrame(data).withColumn("test3",lit(null))
//  val df1=df.select(col("id"),explode(col("attributes")).alias(""))

//df.union()

  def udf2(x:Int):Int={
    x+1
  }
  val plusOneIntx=spark.udf.register("plusOneIntx",udf2(_:Int):Int)


//  df2.explain(extended=true)
//  df2.show()
  println(df.queryExecution.analyzed.output)
//  df.withColumn()
  stopTheThread()


}

object udfWithNULL extends App with utils {

}

object checkDelta extends App {
  def writeSampleDeltaTable(outputPath: String): Unit = {
    val spark = SparkSession.builder()
      .config("spark.jars.packages", "io.delta:delta-core_2.12:3.3.0")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .appName("chapter_3_check_delta_example")
      .master("local[*]")
      .getOrCreate()

    val df = spark.range(10)
    df.write.format("delta").mode("overwrite").save(outputPath)
  }

  writeSampleDeltaTable("/tmp/delta-table")

}

object example3 extends App {

  case class RowPanda(id: Long, zip: String, pt:String, happy: Boolean, attributes: Map[String, String])
  case class PandaPlace(name: String, pandas: Seq[RowPanda])



  val pandasSeq=Seq(
    PandaPlace("San Diego Zoo", Seq(
      RowPanda(1, "92101", "Giant", true, Map("color" -> "black and white", "age" -> "5")),
      RowPanda(2, "92102", "Red", false, Map("color" -> "red", "age" -> "3"))
    )),
    PandaPlace("Smithsonian National Zoo", Seq(
      RowPanda(3, "20008", "Giant", true, Map("color" -> "black and white", "age" -> "4"))
    )),
    PandaPlace("Chengdu Research Base", Seq(
      RowPanda(4, "610041", "Giant", true, Map("color" -> "black and white", "age" -> "6")),
      RowPanda(5, "610042", "Red",true, Map("color" -> "red"))
    ))
  )

  val spark = SparkSession.builder()
    .appName("chapter_3_nested_example")
    .master("local[*]")
    .getOrCreate()


  import spark.implicits._


  val pandas=spark.createDataFrame(pandasSeq)

  println(pandas.printSchema())
  val s1=pandas.map(
    row=>{
      val name=row.getAs[String]("name")
      val arr=row(1).asInstanceOf[Array[RowPanda]].map(
        r=>{
          val happy=r.happy
          val age=r.id
          s"Happy: $happy , Age: $age"
        }
      ).mkString(" | ")
      s"Panda Place: $name has  pandas count: ${arr}"

    }
  )

  s1.explain(true)
  val pandasExp=pandas.withColumn("pandas",explode(col("pandas")))
  val df2_temp=spark.range(1,10,2)
  val df2=df2_temp.filter(col("id")%2===1)
  val df3=pandasExp.join(df2, pandasExp("pandas.id")===df2("id"),"left")

  df3.explain(true)

  pandas
//  s1.explain(true)
//  s1.show(truncate = false)

//
//  print(pandas.schema)
//
//  pandas.select(col("pandas").getItem(1)).show(false)
//
//  pandas.withColumn("pandas",explode(col("pandas"))).select(col("pandas.happy"),col("pandas.attributes.age").alias("age")
//  ,col("pandas.attributes.age").alias("age")=== lit(6),col("pandas.attributes.age").alias("age") <=> lit(6)).show(false)
//
//
//  pandas.withColumn("pandas",explode(col("pandas"))).select(col("pandas.happy"),col("pandas.attributes.age").alias("age"))
//    .filter(col("age")<=> lit(6)).show(false)
//
//  pandas.flatMap()
//  val columnP=explode(col("pandas"))
//
//  columnP.e



}

object example4 extends App {

  val spark = SparkSession.builder()
    .appName("chapter_3_nested_example")
    .master("local[*]")
    .getOrCreate()


  val schema=StructType(
    Array(
      StructField("id",IntegerType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true),
      StructField("mark_english",IntegerType,true),
      StructField("mark_maths",IntegerType,true),
      StructField("mark_science",IntegerType,true)
    ))
  val df=spark.read.option("header","true").schema(schema).json("/home/sanskar/Project/Spark-test/spark-3.5-test/Data/student")
ArrayType

  val marks_arr=df.select(col("id"),array(col("mark_english"),col("mark_maths"),col("mark_science")).alias("marks_array")  )

  val marks_bonus=marks_arr.withColumn("marks_array_bonus",
    expr("transform(marks_array, x -> x + 5 )")
  )



  marks_arr.show()
  println(marks_arr.schema)

}




class Transformation extends utils  {

  def runFunction(spark: SparkSession):Unit={

    println(spark.sparkContext.getConf.get("spark.app.name"))


    val schema=StructType(
      Array(
        StructField("id",IntegerType,true),
        StructField("backdrop_path",StringType,true),
        StructField("recommendations",StringType,true)
      )
    )

    val df=spark
      .read
      .option("header","true")
      .option("mode","PERMISSIVE")
      .option("columnNameOfCorruptRecord","_corrupt_record")
      .option("ignoreTrailingWhiteSpace","true")
      .schema(schema)
      .csv("/home/sanskar/Project/Spark-test/spark-3.5-test/Data/movies_output")
      .limit(10)
    println(df.schema)
    df.show(truncate=false)

//    df.createOrReplaceTempView("movies_output")

//    spark.catalog.listCatalogs().show()
//    spark.catalog.listTables().show()

//    df.select(when(col("id")%2===0,"Even").otherwise("Odd").alias("id_type")).show()




//    df
//      .write
//      .option("header","true")
//      .mode("overwrite")
//      .csv("/home/sanskar/Project/Spark-test/spark-3.5-test/Data/movies_output")
//    val columns= df.columns
//    println(columns)
//
//    df.show(truncate=false)
//    val df2=df.withColumn("generes_split", split(col("genres"), "[\\|\\-,\\s]+"))
//      .withColumn("generes_exploded", explode(col("generes_split")))
//      .groupBy("generes_exploded")
//      .agg(count("title").alias("title_count"),avg(col("budget").cast("double")).alias("avg_budget"))
//
//    val df3=df2.sort(col("title_count").asc_nulls_last,col("generes_exploded").asc_nulls_last)
//
//    df3.explain(true)
//    df3.write.mode("overwrite").parquet("/home/sanskar/Project/Spark-test/spark-3.5-test/output/genre_count_avg_budget_parquet")


    stopTheThread()
  }

}


object myApp extends App with  utils {
  val transformation=new Transformation()
  def calculateTime(func: => Unit): Unit = {
    val startTime = System.currentTimeMillis()
    func
    val endTime = System.currentTimeMillis()
    println(s"Execution time: ${endTime - startTime} ms")
  }
  calculateTime(transformation.runFunction(sparkSessionInit()))
//  calculateTime(transformation.runFunction(sparkSessionInit()))
}





//  df2.show()
//  df.cache()
//  println(s"Count of dataframe ${df.count()}")
//  val rdd=df.rdd
//  println(s"Count coming from rdd operation :- ${rdd.count()}")






