
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.xml._
import scala.io.Source


object SparkApp {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark = SparkSession
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


    val argFile = XML.load(args(0))

    //val argFile =args(0)
  //  val argumentFile = Source.fromFile(argFile)
  //  val argLines = argumentFile.mkString.split("\n")
  val sourceDirectory=  (argFile \ "sourceDirectory").text
  val destinationDirectory = (argFile \ "destinationDirectory").text
  val tracePath = (argFile \ "tracePath").text
    val traceFileName = (argFile \ "traceFileName").text
  val schemaFile = (argFile \ "schemaFile").text
  val resultFile = (argFile \ "resultFile").text
  val readMode = (argFile \ "readMode").text
  val partitionColumn = (argFile \ "partitionColumn").text

println(sourceDirectory)


   val dataFrameTrace =TraceCopyFiles.tracedCopy(sourceDirectory, destinationDirectory, tracePath, traceFileName)

   // val tracefileAsDF=TraceCopyFiles.traceFileToDF(tracePath,traceFileName)
    dataFrameTrace.createOrReplaceTempView("my_temp_table")
    spark.sql("drop table if exists my_table")
    spark.sql("create table my_table as select * from my_temp_table")
    spark.sql("select * from  my_table ").show()

    val schema = ProcessDataFiles.parsingSchema( schemaFile)
    val processedData:DataFrame = ProcessDataFiles.process(destinationDirectory, schema, readMode)
    ProcessDataFiles.write(processedData, partitionColumn, resultFile)


    Thread.sleep(100000000)

}



}




