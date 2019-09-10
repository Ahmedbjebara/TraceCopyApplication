
import java.io.{File, FileInputStream, FileWriter}
import java.nio.file.{Files, Paths}
import java.security.{DigestInputStream, MessageDigest}

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.text.SimpleDateFormat

import scala.io.Source


object TraceCopyFiles {

  val spark = SparkSession
    .builder()
    .appName("SparkSchema")
    .config("spark.master", "local[*]")
    .getOrCreate()


  def tracedCopy(sourceDirectory: String, destinationDirectory: String, tracePath: String,traceFileName: String): Unit = {

    val traceFilePath = tracePath + traceFileName
    val header : String = "File;Source;Destination;State;Cheksum;Message;Size;LastModifiedDate"

    def listFilesFrom(directory: String): List[File] = {
      val directoryFile: File= new File (directory)
      if (directoryFile.exists && directoryFile.isDirectory) {
        directoryFile.listFiles.filter(_.isFile).toList
      } else {
        List[File]()
      }
    }


    def isFileEmpty(file: String) = Source.fromFile(file).isEmpty



    def initTraceFile(tracePath: String, traceFileName: String, header: String): Unit = {
      if (isFileEmpty(traceFilePath)) {
        val headerWriter = new FileWriter(new File(traceFilePath), true)
        headerWriter.write(header + String.format("%n"))
        headerWriter.close()
      }
    }


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


    def getTracedFilesChecksum(tracePath: String, traceFileName: String): Array[String] = {
      // if (!isFileEmpty(traceFilePath)){
      val traceFile = spark.read.option("delimiter", ";").option("header", "true").csv(traceFilePath)
      traceFile
        .select("Cheksum")
        .collect()
        .map(x => x.getString(0))

    }


    val fileList = listFilesFrom(sourceDirectory)  // 1 APPEL
    // TODO: useless rdd
    val fileRdd = spark.sparkContext.parallelize(fileList)



    initTraceFile(tracePath, traceFileName, header)   // 2 APPEL


    val tracedFilesChecksum = getTracedFilesChecksum(tracePath, traceFileName) // 3 APPEL




    def traceWriter(traceFileWriter: FileWriter, traceFilePath: String, message: String)={
      traceFileWriter.write(message)
    }


    // case class TracedMoveAction(file: File, sourceDirectory: String, destinationDirectory: String,traceFilePath: String,
    //   tracedFilesChecksum : Array[String])
    //  {
    def tracedMove(file: File, sourceDirectory: String, destinationDirectory: String,traceFilePath: String,
                   tracedFilesChecksum : Array[String]) = {



      val length = file.length()
      val sdateformat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
      val lastmodifieddate = sdateformat.format(file.lastModified())


      val traceFileWriter = new FileWriter(new File(traceFilePath), true)

      //  val traceFileWriter = new FileWriter(new File(traceFilePath), true)
      val hash = computeHash(sourceDirectory + file.getName) //check du nveau fichier  // APPEL
      val exists = Files.exists(Paths.get(destinationDirectory + file.getName)) // true ou false existe dans le rep destination

      //tracedFilesChecksum.foreach(x=>println(x))


      (exists, tracedFilesChecksum.contains(hash)) match{
        case(false, false) => {

          Files.move(
            Paths.get(sourceDirectory+ file.getName),
            Paths.get(destinationDirectory + file.getName))
          val   messageSuccess = file.getName + ";" + file.getAbsolutePath + ";" + destinationDirectory + file.getName + ";MOVE SUCCESS: File's Name  dosen't exist yet !" + ";" + hash + ";Cheksum dosen't exist yet !"+ ";"+ length + ";"+ lastmodifieddate+ String.format("%n")
          traceWriter(traceFileWriter,traceFilePath,messageSuccess )

        }

        case(true, false) => {

          val messageFileNameExists= file.getName + ";" + file.getAbsolutePath + ";" + destinationDirectory + file.getName + ";MOVE FAILED: File's Name Already Exists" + ";" + hash + "; " + ";"+ length + ";"+ lastmodifieddate+ String.format("%n")
          traceWriter(traceFileWriter,traceFilePath,messageFileNameExists )

        }

        case(false, true) => {

          val messageChecksumExists=file.getName + ";" + file.getAbsolutePath + ";" + destinationDirectory+ file.getName + ";MOVE FAILED" + ";" + hash + ";Cheksum Exists Already !" + ";"+ length + ";"+ lastmodifieddate+ String.format("%n")
          traceWriter(traceFileWriter, traceFilePath,messageChecksumExists )

        }

        case(true , true) => {

          val messageChecksum_NameExists=file.getName + ";" + file.getAbsolutePath + ";" + destinationDirectory+ file.getName + ";MOVE FAILED" + ";" + hash + ";Cheksum AND Name Exists Already !" + ";"+ length + ";"+ lastmodifieddate+ String.format("%n")
          traceWriter(traceFileWriter, traceFilePath,messageChecksum_NameExists )


        }
      }
      traceFileWriter.close()

    }
    //}



 //def writeIntoHiveTable(file: File, sourceDirectory: String, destinationDirectory: String)={
  //  val hash = computeHash(sourceDirectory + file.getName)
//  val sdateformat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
 // val lastmodifieddate = sdateformat.format(file.lastModified())
    //spark.sqlContext.sql("INSERT INTO TABLE employeeHive " +
    //  (file.getName , file.getAbsolutePath , destinationDirectory  , "MOVE SUCCESS: File's Name  dosen't exist yet !" , hash, "Cheksum dosen't exist yet !", file.length , lastmodifieddate)
   // )
//}





    fileRdd.foreach(file =>
    {
      tracedMove(file, sourceDirectory, destinationDirectory,traceFilePath, tracedFilesChecksum) // 4 APPEL
      //writeIntoHiveTable(file, sourceDirectory, destinationDirectory)
    }

    )
  }

  def traceFileToDF(tracePath: String, traceFileName: String):DataFrame ={
   val traceFilePath=tracePath+traceFileName
     spark.read.option("delimiter", ";").option("header", "true").csv(traceFilePath)

  }
}
