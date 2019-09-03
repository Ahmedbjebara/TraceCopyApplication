import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, StringType, StructField, StructType}
import scala.io.Source


object CsvSchemaParser {


  def parseCsvFileToSchema (schemaFile : String):StructType =
  {

    println(schemaFile)

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
