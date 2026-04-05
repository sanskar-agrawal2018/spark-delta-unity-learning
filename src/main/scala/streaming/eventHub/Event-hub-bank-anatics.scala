package streaming.eventHub

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import java.io.File


object Spark4OnlyPipeExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark 4 PIPE Operator Example")

      .master("local[*]")
      .getOrCreate()


    val TOPIC = "bank-accounts-data"
    val BOOTSTRAP_SERVERS = "bank-analytics.servicebus.windows.net:9093"

    // Load secret from conf/secrets.conf (git-ignored) or fallback to environment variable EH_SASL
    val secretsFile = new File("conf/secrets.conf")
    val config = if (secretsFile.exists()) ConfigFactory.parseFile(secretsFile).resolve()
                 else ConfigFactory.load()

    val EH_SASL = if (config.hasPath("eh-sasl")) config.getString("eh-sasl") else sys.env.getOrElse("EH_SASL", "")

    if (EH_SASL.isEmpty) {
      System.err.println("WARNING: EH_SASL is empty. Provide conf/secrets.conf or set EH_SASL environment variable.")
    }

    val jaas_conf = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"" + EH_SASL + "\";"
    // Mask secret when printing
    val masked = if (EH_SASL.length > 8) EH_SASL.take(4) + "..." + EH_SASL.takeRight(4) else EH_SASL
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
        .option("startingOffsets", "earliest")
        .load()

    val df_write = df.writeStream
  .outputMode("append")
  .format("console")
  .start()




    df_write.awaitTermination()
  }
}
