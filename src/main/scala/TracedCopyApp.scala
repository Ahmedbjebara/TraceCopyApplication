import org.apache.spark.sql.{DataFrame, SparkSession}


object TracedCopyApp {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")

    implicit val spark = SparkSession
      .builder()
      .appName("SparkSchema")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()

    if (args.length < 1) {
      System.err.println(
        "Argument number's is not respected")
      System.exit(1)
    }


    val config :Config = ArgFileConf.loadConfig(args(0))



      config.traceMethod match {
        case "file" => CopyFileService.tracedCopy(config.sourceDirectory, config.destinationDirectory, config.tracePath, config.traceFileName)
       case "hivetable" => {spark.sql("DROP TABLE TRACETABLE")
         CopyHiveTableService.tabletracedCopy(config.sourceDirectory, config.destinationDirectory)
         spark.sql("SELECT * FROM TRACETABLE").show()}

        case "kafkatopic" =>{ CopyKafkaTopicService.CopyKafkaTopicTableService(config.sourceDirectory, config.destinationDirectory)
      val  df =spark.sql("SELECT File FROM TRACETABLE").toDF()


      df.selectExpr( "CAST(File AS STRING) AS value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers","127.0.0.1:9092")
        .option("topic","ahmedd4")
        .save()

      val df1 = spark
        .read
        .format("kafka")
        .option("kafka.bootstrap.servers", "127.0.0.1:9092")
        .option("subscribe", "ahmedd4")
        .load()

      df1.printSchema()
      df1.show(true) }
      }



    val schema = ProcessDataFiles.parseSchema(config.schemaFile)
    val loadedData: DataFrame = ProcessDataFiles.load( config.destinationDirectory ,schema, config.readMode, config.sourceFileFormat)
    val processedData: DataFrame = ProcessDataFiles.process( loadedData)
    ProcessDataFiles.write(processedData, config.partitionColumn, config.resultFile)


    Thread.sleep(100000000)

  }
}




