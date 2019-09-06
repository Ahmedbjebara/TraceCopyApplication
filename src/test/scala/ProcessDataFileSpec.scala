import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class ProcessDataFileSpec extends FlatSpec with GivenWhenThen with Matchers{

  behavior of "CsvSchemaParser"

val ReferenceSchema = StructType(
  StructField("_c0", IntegerType, false) ::
    StructField("carat", StringType, false) ::
    StructField("cut", StringType, false) ::
    StructField("color", StringType, false) ::
    StructField("clarity", StringType, false) ::
    StructField("depth", DoubleType, false) ::
    StructField("table", StringType, false) ::
    StructField("price", StringType, false) ::
    StructField("x", DoubleType, false) ::
    StructField("y", DoubleType, false) ::
    StructField("z", DoubleType, false) :: Nil)

  val destinationDataType=IntegerType

  it should "parse csv file to schema" in {
    Given ("an absolute path of a csv file")
    val path = "C:/Users/dell/IdeaProjects/Realtimecheksum/projet/schema/schemaout.csv"
    When ("parseCsvFileToSchema is invoked")
   val resultatSchema =  ProcessDataFiles.parseCsvFileToSchema(path)
    Then (" a schema should be returned ")
    resultatSchema should equal (ReferenceSchema)

  }

  it should "replace type with datatype" in {
    Given ("a string")
    val sourceType= "Integer"
    When ("replaceTypeToDatatype is invoked")
    val resultatDataType =  ProcessDataFiles.replaceTypeWithDatatype(sourceType)
    Then (" a datatype should be returned ")
    resultatDataType should equal (destinationDataType)

  }

}
