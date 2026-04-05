
import org.scalatest.funsuite.AnyFunSuite
import highPerformanceSpark.Chapter_3.checkDelta
import highPerformanceSpark.utils.utils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructType,StructField,IntegerType,LongType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.nio.file.Files


case class PrimeResult(number: Int, is_prime: Boolean)

trait testUtils {
  def compareDf(spark:SparkSession, df1:DataFrame, df2:DataFrame): Boolean = {
    df1.schema == df2.schema &&
    df1.collect().toSet == df2.collect().toSet
  }
}


class testUnits extends AnyFunSuite with utils  with testUtils {

  test("test stieve function for n=30") {
    val expectedPrimes = Seq(2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
    assert(stieve(30) == expectedPrimes)
  }

  test("test isPrime") {
    val spark = sparkSessionInit()
    val df=spark.range(10).toDF("number")
    val df2=isPrime(spark,df,col("number"))

    val expectedDF=spark.createDataFrame(Seq(
      PrimeResult(0, false),
      PrimeResult(1, false),
      PrimeResult(2, true),
      PrimeResult(3, true),
      PrimeResult(4, false),
      PrimeResult(5, true),
      PrimeResult(6, false),
      PrimeResult(7, true),
      PrimeResult(8, false),
      PrimeResult(9, false)



    ))

    expectedDF.printSchema()

    val expectedDF2=expectedDF.withColumn("number",col("number").cast(LongType))

    assert(df2.schema==expectedDF2.schema)

    assert(df2.collect().toSet==expectedDF2.collect().toSet)
  }

  test("checkDelta writes a delta table with 10 rows") {
    val spark = sparkSessionInit()
    val outputPath = Files.createTempDirectory("check-delta-test-").toFile.getAbsolutePath

    checkDelta.writeSampleDeltaTable(outputPath)

    val actualDf = spark.read.format("delta").load(outputPath)
    val expectedDf = spark.range(10).toDF("id")

    assert(actualDf.count() == 10)
    assert(compareDf(spark, actualDf, expectedDf))
  }
}
