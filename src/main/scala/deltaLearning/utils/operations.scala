package deltaLearning.utils

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, LongType}
import org.apache.spark.sql.functions.{col, e, explode}
import org.apache.spark.sql.{DataFrame, SaveMode}
trait  operations {
  def  createDeltaTable(spark:SparkSession,readPath:String,format:String,loadPath:String):DeltaTable={
    val df=spark.read.format(format).load(readPath)
    df.write.format("delta").mode("overwrite").save(loadPath)
    DeltaTable.forPath(loadPath)
  }



  def detailDeltaTable(spark:SparkSession,deltaTable:DeltaTable):Unit={


    spark.sparkContext.setLogLevel("ERROR")
    deltaTable.detail().show(truncate = false)
    deltaTable.history().show(truncate = false)
    deltaTable.toDF.show(truncate = false)

    spark.sparkContext.setLogLevel("INFO")


  }




  def getDeltaTable(loadPath:String):DeltaTable={
    DeltaTable.forPath(loadPath)
  }



  def stopTheThread (): Unit ={
    println("Press ENTER to stop the application...")
    scala.io.StdIn.readLine()
  }

  def createDeltaWithPath (spark:SparkSession,location:String):DeltaTable={
    DeltaTable.createIfNotExists(spark)
      .addColumn("firstName", "STRING")
      .addColumn(
        DeltaTable.columnBuilder("gender")
          .dataType("STRING")
            
          .comment("sdd")
          .build()
      )

      .addColumn(
        DeltaTable.columnBuilder("dateOfBirth")
          .dataType(DateType)
          .build()
      )
      .addColumn(
        DeltaTable.columnBuilder("id")
          .dataType(LongType)
          .generatedAlwaysAsIdentity(start=1L,step=3L)
          .build()
      )
      .addColumn(
        DeltaTable.columnBuilder("fullName")
          .dataType("STRING")
          .generatedAlwaysAs(s"concat(firstName,'_',gender)")
          .build()
      )
      .partitionedBy("gender")
      .location(location)
      .execute()

  }

  def appendDeltaTable(spark:SparkSession,deltaTable:DeltaTable,readPath:String,format:String):Unit={
    val df=spark.read.format(format).load(readPath)
    df.limit(10).write.format("delta").mode("append").save(deltaTable.detail().select("location").collect()(0).getString(0))



//    val df_stream=spark.readStream.format(format).load(readPath)

    //add event listerner to monitor the streaming query
    class QueryListener extends org.apache.spark.sql.streaming.StreamingQueryListener {
      override def onQueryStarted(event: org.apache.spark.sql.streaming.StreamingQueryListener.QueryStartedEvent): Unit = {
        println(s"Query started: ${event.id}, name: ${event.name}, runId: ${event.runId}")
      }

      override def onQueryIdle(event: org.apache.spark.sql.streaming.StreamingQueryListener.QueryIdleEvent): Unit = {
        println(s"Query idle: ${event.id} runId: ${event.runId}")
      }

      override def onQueryProgress(event: org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent): Unit = {
        println(s"Query made progress: ${event.progress.prettyJson}")
      }

      override def onQueryTerminated(event: org.apache.spark.sql.streaming.StreamingQueryListener.QueryTerminatedEvent): Unit = {
        println(s"Query terminated: ${event.id}, name: , runId: ${event.runId}, exception: ${event.exception.getOrElse("None")}")
      }
    }
//    spark.streams.addListener(new QueryListener() )

//    val query=df_stream.writeStream
//      .format("delta")
//      .option("checkpointLocation",s"${deltaTable.detail()operations.select("location").collect()(0).getString(0)}_checkpoints/stream1")
//      .outputMode("append")
//      .start(deltaTable.detail().select("location").collect()(0).getString(0))
//      .awaitTermination()

//    def appendDfAndWrite(df:org.apache.spark.sql.DataFrame,count:Int):DataFrame={
//      if(count==0){
//        return df;
//      }
//      println(s"Df count: ${df.count()}")
//
//
//     deltaTable.history().limit(1).show(truncate = false)

//      appendDfAndWrite(df.unionAll(df),count-1)
//    }

//    val deltaPath=deltaTable.detail().select("location").collect()(0).getString(0)
//    println(s"Appending data to Delta Table at path: ${deltaPath}")
//    val df1=appendDfAndWrite(df,10)
////    println(s"Final Df count: ${df1.count()}")
//    for(i<-1 to 1001){
//      println(s"Appending data to Delta Table at path: ${deltaPath}, iteration: ${i}")
//       df1.write.option("optimizeWrite",true).format("delta").mode("append").save(s"${deltaPath}_raw")
//
//      val deltaTable1=DeltaTable.forPath(s"${deltaPath}_raw")
//      spark.sql(s"DESCRIBE DETAIL delta.`${deltaPath}_raw`").show(truncate = false)
//    }

  }

}
