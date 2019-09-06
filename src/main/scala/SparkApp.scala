import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.io.Source


object SparkApp {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark = SparkSession
    .builder()
    .appName("SparkSchema")
    .config("spark.master", "local[*]")
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
    val schema = ProcessDataFiles.parsingSchema( schemaFile)

    val processedData:DataFrame = ProcessDataFiles.process(destinationDirectory, schema, readMode)
    ProcessDataFiles.write(processedData, partitionColumn, resultFile)


    Thread.sleep(100000000)

}
}




