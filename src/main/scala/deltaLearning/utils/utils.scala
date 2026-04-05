package deltaLearning.utils

import org.apache.spark.sql.SparkSession
class Deltautils {
  def getSparkSession(): SparkSession = {
    import org.apache.spark.sql.SparkSession
    import delta._
    import io.delta.sql.DeltaSparkSessionExtension

    val spark = SparkSession.builder()
      .appName("DeltaLearningUtils")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")


      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

      
      .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")
    println(s"Spark Version: ${spark.version}")
    println(s"Delta Lake Version: ")
   println(s"Delta Version: ${io.delta.VERSION}")
    spark
  }




  def stopTheThread (): Unit ={
    println("Press ENTER to stop the application...")
    scala.io.StdIn.readLine()
  }


}



