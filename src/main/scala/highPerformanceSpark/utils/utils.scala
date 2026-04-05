package highPerformanceSpark.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
// scala
case class Transaction(id: Int, amount: Double)
// scala
case class Product(id: Int, name: String, price: Double)// scala
case class User(id: Int, name: String, transactions: Seq[Transaction], products: Seq[Product])
trait utils {
  def sparkSessionInit(): SparkSession = {
    import org.apache.spark.sql.SparkSession

//    val spark = SparkSession.builder()
//      .appName("DeltaLearningUtils")
//      .master("local[*]")
//      .config("spark.jars.packages", "io.delta:delta-core_2.12:3.3.0")
//      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
////      .config("spark.memory.offHeap.enabled", "true")
////      .config("spark.memory.offHeap.size", "4g")
//      .getOrCreate()

    val spark = SparkSession.builder()
      .appName("DeltaLearningUtils")
      .master("local[*]")
      .config("spark.jars.packages", "io.delta:delta-core_2.12:3.3.0")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.memory.offHeap.enabled", "true")
            .config("spark.memory.offHeap.size", "4g")
      .getOrCreate()
    println(s"Spark config - ${spark.sparkContext.getConf.get("spark.memory.offHeap.enabled")}")
    println(s"Spark config - ${spark.sparkContext.getConf.get("spark.memory.offHeap.size")}")
    println(s"Spark Version: ${spark.version}")
    spark
  }
  def add(a:Int,b:Int):Int={
    a+b
  }
  def stopTheThread (): Unit ={
    println("Press ENTER to stop the application...")
    scala.io.StdIn.readLine()
  }



  def cleanComplexUserDataFrame(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val productA = Product(1, "ProductA", 50.0)
    val productB = Product(2, "ProductB", 75.5)
    val productC = Product(3, "ProductC", 120.0)
    val productD = Product(4, "ProductD", 180.0)
    val productE = Product(5, "ProductE", 250.5)

    val users: Seq[User] = Seq(
      User(1, "Alice", Seq(Transaction(1, 100.0), Transaction(2, 150.5)), Seq(productA, productB)),
      User(2, "Bob", Seq(Transaction(3, 200.0), Transaction(4, 300.75)), Seq(productC)),
      User(3, "Charlie", Seq(Transaction(5, 400.0)), Seq(productA, productB, productD, productE)),
      User(4, "David", Seq(), Seq()),
      User(5, "Eve", Seq(Transaction(6, 500.0), Transaction(7, 600.25), Transaction(8, 700.5)), Seq(productB, productC, productD)),
      User(6, "Frank", Seq(Transaction(9, 800.0)), Seq(productE))
    )

    val df = spark.createDataset(users).toDF()
    df
  }

  def stieve(n:Int): Seq[Int] = {
    val isPrime = Array.fill(n + 1)(true)
    isPrime(0) = false
    isPrime(1) = false

    for (i <- 2 to math.sqrt(n).toInt if isPrime(i)) {
      for (j <- i * i to n by i) {
        isPrime(j) = false
      }
    }

    isPrime.zipWithIndex.collect { case (true, index) => index }

  }


  /**
   *
   * @param spark
   * @param df
   * @param col
   * @return
   */

  def isPrime(spark:SparkSession, df:DataFrame,col:Column):DataFrame= {
    import org.apache.spark.sql.functions.udf
    val indexedSeq=stieve(1000)
    val df_prime_stieve=spark.createDataFrame(indexedSeq.map(Tuple1(_))).toDF("number")
    val df_isprime=df.join(df_prime_stieve,df("number")===df_prime_stieve("number"),"left").
     select(df("number"),when(df_prime_stieve("number").isNotNull,lit(true))
       .otherwise(lit(false)).alias("is_prime"))
    df_isprime
  }


  def  isPrimeUsingIsIN(spark:SparkSession, df:DataFrame,col:Column):DataFrame= {
    import org.apache.spark.sql.functions.udf
    val indexedSeq = stieve(1000)
    val df_prime_stieve = spark.createDataFrame(indexedSeq.map(Tuple1(_))).toDF("number")
    val df_isprime = df.withColumn("is_prime", col("number").isin(indexedSeq: _*))
    df_isprime
  }

}


object TestUtils extends App with utils {
  val spark = sparkSessionInit()
  val df=spark.range(1000).toDF("number")
  val df2=isPrime(spark,df,col("number"))
  df2.show(truncate=false)



//  println(stieve(100))
  stopTheThread()
}


