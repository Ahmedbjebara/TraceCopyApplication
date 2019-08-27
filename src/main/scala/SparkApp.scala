import org.apache.spark.rdd.RDD
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.io._
import java.security.{DigestInputStream, MessageDigest}
import java.io.{File, FileInputStream}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.io.Source


object SparkApp {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("SparkSchema")
      .config("spark.master", "local[*]")
      .getOrCreate()

    if (args.length < 4) {
      System.err.println(
        "Argument number's is not respected")
      System.exit(1)
    }
    val dataFile: String = args(0)
    val schemaFile: String = args(1)
    val resultFile: String = args(2)
    val readMode: String = args(3)

    var fileList: List[File] = null
    val sourceDirectory: File = new File("C:/Users/dell/IdeaProjects/Realtimecheksum/projet/source/")


    if (sourceDirectory.exists && sourceDirectory.isDirectory) {
      fileList = sourceDirectory.listFiles.filter(_.isFile).toList
    } else {
      fileList = List[File]()
    }

    val file: RDD[File] = spark.sparkContext.parallelize(fileList)


    def computeHash(path: String): String = {
      val buffer = new Array[Byte](8192)
      val md5 = MessageDigest.getInstance("MD5")
      val dis = new DigestInputStream(new FileInputStream(new File(path)), md5)
      try {
        while (dis.read(buffer) != -1) {}
      } finally {
        dis.close()
      }
      md5.digest.map("%02x".format(_)).mkString
    }

    val headerFileWriter = new FileWriter(new File("C:/Users/dell/IdeaProjects/Realtimecheksum/projet/trace/trace.csv"), true)

    if (Source.fromFile("C:/Users/dell/IdeaProjects/Realtimecheksum/projet/trace/trace.csv").isEmpty)
      headerFileWriter.write("File;Source;Destination;State;Cheksum;Message" + String.format("%n"))
    headerFileWriter.close()




    if (Source.fromFile("C:/Users/dell/IdeaProjects/Realtimecheksum/projet/trace/trace.csv").nonEmpty) {
      val traceFile = spark.read.option("delimiter", ";").option("header", "true") csv ("C:/Users/dell/IdeaProjects/Realtimecheksum/projet/trace/trace.csv")
      val arrayChecksum = traceFile.select("Cheksum").collect() //collection : ARRAY of rows
        .map(x => x.getString(0)) //collection : ARRAY of string

      file.foreach(selectedFile => {

        val hash: String = computeHash("C:/Users/dell/IdeaProjects/Realtimecheksum/projet/source/" + selectedFile.getName)
        val fileWriter = new FileWriter(new File("C:/Users/dell/IdeaProjects/Realtimecheksum/projet/trace/trace.csv"), true)
        val exists = Files.exists(Paths.get("C:/Users/dell/IdeaProjects/Realtimecheksum/projet/destination/" + selectedFile.getName))


        if ((!exists) && !arrayChecksum.contains(hash)) {
          val path = Files.move(
            Paths.get(selectedFile.getAbsolutePath),
            Paths.get("C:/Users/dell/IdeaProjects/Realtimecheksum/projet/destination/" + selectedFile.getName), StandardCopyOption.REPLACE_EXISTING)

          fileWriter.write(selectedFile.getName + ";" + selectedFile.getAbsolutePath + ";" + "C:/Users/dell/IdeaProjects/Realtimecheksum/projet/destination/"
            + selectedFile.getName + ";MOVE SUCCESS: File's Name  dosen't exist yet !" + ";" + hash + ";Cheksum dosen't exist yet !" + String.format("%n"))

        }
        else {

          if (arrayChecksum.contains(hash) && (!exists)) {
            fileWriter.write(selectedFile.getName + ";" + selectedFile.getAbsolutePath + ";" + "C:/Users/dell/IdeaProjects/Realtimecheksum/projet/destination/"
              + selectedFile.getName + ";MOVE FAILED" + ";" + hash + ";Cheksum Exist Already !" + String.format("%n"))
          }
          else {
            fileWriter.write(selectedFile.getName + ";" + selectedFile.getAbsolutePath + ";" + "C:/Users/dell/IdeaProjects/Realtimecheksum/projet/destination/"
              + selectedFile.getName + ";MOVE FAILED: File's Name Already Exists"
              + ";" + hash + "; " + String.format("%n"))
          }
        }

        fileWriter.close()
      })


    }
    else
      println("Trace file is empty!!!!!")

    val schemaParse = CsvSchemaParser.parseCsvFileToSchema(schemaFile)

    val dataFrameWithParsedSchema = spark.read.format("csv")
      .option("header", "true")
      .option("mode", readMode)
      .schema(schemaParse)
      .load(dataFile)


    spark.udf.register("file_name", (path: String) => path.substring(path.lastIndexOf("/") + 1, path.lastIndexOf(".")))

    val dataFrameWithFilename: DataFrame = dataFrameWithParsedSchema.withColumn("fileName", callUDF("file_name", input_file_name()))

    dataFrameWithFilename.write
      .partitionBy("filename")
      .parquet(resultFile)

    Thread.sleep(100000000)
  }
}




