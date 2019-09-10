import TraceCopyFiles.spark
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
import org.apache.spark.sql.{DataFrame, SparkSession}

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

    val argFile =args(0)
    val argumentFile = Source.fromFile(argFile)
    val argLines = argumentFile.mkString.split("\n")

  val sourceDirectory: String= argLines(0).trim
  val destinationDirectory: String = argLines(1).trim
  val tracePath: String = argLines(2).trim
    val traceFileName: String = argLines(3).trim
  val schemaFile: String = argLines(4).trim
  val resultFile: String = argLines(5).trim
  val readMode: String = argLines(6).trim
  val partitionColumn: String = argLines(7).trim


   TraceCopyFiles.tracedCopy(sourceDirectory, destinationDirectory, tracePath, traceFileName)
    val tracefileAsDF=TraceCopyFiles.traceFileToDF(tracePath,traceFileName)
    tracefileAsDF.createOrReplaceTempView("my_temp_table")
    spark.sql("drop table if exists my_table")
    spark.sql("create table my_table as select * from my_temp_table")
    spark.sql("select * from  my_table ").show()

    val schema = ProcessDataFiles.parsingSchema( schemaFile)
    val processedData:DataFrame = ProcessDataFiles.process(destinationDirectory, schema, readMode)
    ProcessDataFiles.write(processedData, partitionColumn, resultFile)

   // spark.sqlContext.sql("TRUNCATE TABLE  TraceMoveApp")
   // spark.sqlContext.sql("CREATE TABLE IF NOT EXISTS TraceMoveApp(File STRING ,Source STRING ,Destination STRING ,State STRING , Cheksum STRING ,Message STRING ,Size STRING , LastModifiedDate String ) row format delimited fields terminated BY ';' lines terminated BY '\n' ")
    //spark.sqlContext.sql("load data inpath 'C:/Users/dell/IdeaProjects/Realtimecheksum/projet/trace/trace.csv'   into table TraceMoveApp" )
   // spark.sqlContext.sql("SELECT * FROM TraceMoveApp").show()

    Thread.sleep(100000000)

}



}




