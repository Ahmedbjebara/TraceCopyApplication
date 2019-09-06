
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.io.Source


object ProcessDataFiles {

  val spark = SparkSession
    .builder()
    .appName("SparkSchema")
    .config("spark.master", "local[*]")
    .getOrCreate()

  def parsingSchema( schemaFile:String) =
{
  parseCsvFileToSchema(schemaFile)
}

  def process(destinationDirectory:String, schemaParse:StructType, readMode:String):DataFrame=
  {

     spark.read.format("csv")
      .option("header", "true")
      .option("mode", readMode)
      .schema(schemaParse)
      .load(destinationDirectory)
  }


  def write(dataFrameWithParsedSchema : DataFrame, partitionColumn: String, resultFile:String)
  {
    spark.udf.register("file_name", (path: String) => path.substring(path.lastIndexOf("/") + 1, path.lastIndexOf(".")))
      val dataFrameWithFilename: DataFrame = dataFrameWithParsedSchema.withColumn("fileName", callUDF("file_name", input_file_name()))

    dataFrameWithFilename.write
      .partitionBy(partitionColumn)
      .parquet(resultFile)
  }

  def parseCsvFileToSchema (schemaFile : String):StructType =
  {


    val sourceFile = Source.fromFile(schemaFile).mkString
    val schematoArrayofString = sourceFile.split(",").map(line=>
    {
      val name =line.substring(line.indexOf("\"") + 1, line.indexOf(("\""), line.indexOf("\"") + 1))
      val typeField = line.substring(line.indexOf(("\""), line.indexOf(":")) + 1, line.lastIndexOf("\""))

      StructField(name,replaceTypeWithDatatype(typeField),false)

    })
    StructType.apply(schematoArrayofString)

  }



  def replaceTypeWithDatatype(typeName: String):DataType = {
    typeName match {
      case "String" => StringType
      case "Integer" => IntegerType
      case "Double" => DoubleType
      case "Boolean" => BooleanType

    }
  }

}
