import org.apache.spark.sql.SparkSession
import org.apache.kafka.common.security.plain.PlainLoginModule
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.{col,expr}
import com.typesafe.config.ConfigFactory
import java.io.File


object Spark4OnlyPipeExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark 4 PIPE Operator Example")

      .master("local[*]")
      .getOrCreate()

    StructType


    val TOPIC = "telematic-stream-topic-1"
    val BOOTSTRAP_SERVERS = "telematic-data.servicebus.windows.net:9093"

    // Load EH_SASL from conf/secrets.conf (git-ignored) or environment variable EH_SASL
    val secretsFile = new File("conf/secrets.conf")
    val config = if (secretsFile.exists()) ConfigFactory.parseFile(secretsFile).resolve() else ConfigFactory.load()

    val EH_SASL = if (config.hasPath("eh-sasl")) config.getString("eh-sasl") else sys.env.getOrElse("EH_SASL", "")

    if (EH_SASL.isEmpty) System.err.println("WARNING: EH_SASL is empty. Provide conf/secrets.conf or set EH_SASL environment variable.")

    val jaas_conf = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"" + EH_SASL + "\";"
    val masked = if (EH_SASL != null && EH_SASL.length > 8) EH_SASL.take(4) + "..." + EH_SASL.takeRight(4) else EH_SASL
    println("jaas_conf (masked): username=\"$ConnectionString\" password=\"" + masked + "\";")
    val df = spark.readStream
        .format("kafka")
      .option("subscribe", TOPIC)
  .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
  .option("kafka.sasl.mechanism", "PLAIN")
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.jaas.config", jaas_conf)
  .option("kafka.request.timeout.ms", "60000")
  .option("kafka.session.timeout.ms", "30000")
  .option("failOnDataLoss", "true")
  .load()

    val df_write = df.writeStream
  .outputMode("append")
  .format("console")
  .start()




    df_write.awaitTermination()
  }
}



