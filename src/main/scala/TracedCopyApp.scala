

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, types}

import scala.xml._


object TracedCopyApp {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")

    implicit val spark = SparkSession
      .builder()
      .appName("SparkSchema")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()

    if ( args.length < 1 ) {
      System.err.println(
        "Argument number's is not respected")
      System.exit(1)
    }


    val config: Config = ArgFileConf.loadConfig(args(0))


    config.traceMethod match {
      case "file" => CopyFileService.tracedCopy(config.sourceDirectory, config.destinationDirectory, config.tracePath, config.traceFileName)
      // case "hivetable" => {spark.sql("DROP TABLE TRACETABLE")
      //   CopyHiveTableService.tabletracedCopy(config.sourceDirectory, config.destinationDirectory)
      //   spark.sql("SELECT * FROM TRACETABLE").show()}
      case "kafkatopic" => {
        CopyKafkaTopicService.tabletracedCopy(config.sourceDirectory, config.destinationDirectory)
        val df = spark.sql("SELECT * FROM TRACETABLE").toDF()
        df.show()

        val schema = new StructType()
          .add("File", StringType)
          .add("Source", StringType)
          .add("Destination", StringType)
          .add("State", StringType)
          .add("Cheksum", StringType)
          .add("Message", StringType)
          .add("Size", StringType)
          .add("LastModifiedDate", StringType)



        import org.apache.spark.sql.functions.to_json
        import spark.implicits._
        import org.apache.spark.sql.functions.struct
        val dfjson = df.select(to_json(struct($"File", $"Source", $"Destination", $"State", $"Cheksum", $"Message", $"Size", $"LastModifiedDate"))as "data")



        dfjson.selectExpr(" data  as value")
         .write
         .format("kafka")
         .option("kafka.bootstrap.servers", "127.0.0.1:9092")
         .option("topic", "nidhal1")
         .save()


        val df1 = spark
          .read
          .format("kafka")
          .option("kafka.bootstrap.servers", "127.0.0.1:9092")
          .option("subscribe", "nidhal1")
          .option("startingOffsets","earliest")
          .load()



        df1.printSchema()
        df1.show(true)

        import  org.apache.spark.sql.functions._
        val df2 = df1.select(from_json(col("value").cast("string"), schema).as("data"))
          .select("data.*")

        df2.printSchema()
        df2.show(true)



      }
      case _ => println("***")
    }


  //  val schema = ProcessDataFiles.parseSchema(config.schemaFile)
  //  val loadedData: DataFrame = ProcessDataFiles.load(config.destinationDirectory, schema, config.readMode, config.sourceFileFormat)
  //  val processedData: DataFrame = ProcessDataFiles.process(loadedData)
 //   ProcessDataFiles.write(processedData, config.partitionColumn, config.resultFile)


    Thread.sleep(100000000)

  }
}




