
import java.io.{File, FileInputStream, FileWriter}
import java.nio.file.{Files, Paths}
import java.security.{DigestInputStream, MessageDigest}

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.text.SimpleDateFormat


import scala.io.Source


object TraceCopyFiles {

  val spark = SparkSession
    .builder()
    .appName("SparkSchema")
    .config("spark.master", "local[*]")
    .getOrCreate()

  def tracedCopy(sourceDirectory: String, destinationDirectory: String, tracePath: String, traceFileName: String): DataFrame = {
    import spark.implicits._
    val traceFilePath = tracePath + traceFileName
    val header: String = "File;Source;Destination;State;Cheksum;Message;Size;LastModifiedDate"

    val fileList = listFilesFrom(sourceDirectory) // 1 APPEL
    // TODO: useless rdd
    val fileRdd = spark.sparkContext.parallelize(fileList)
    initTraceFile(tracePath, traceFileName, header) // 2 APPEL
    val tracedFilesChecksum = getTracedFilesChecksum(tracePath, traceFileName) // 3 APPEL

     fileRdd.map(file => {
      tracedMove(file, sourceDirectory, destinationDirectory, traceFilePath, tracedFilesChecksum)
    }).toDF()


  }


  def listFilesFrom(directory: String): List[File] = {
    val directoryFile: File = new File(directory)
    if ( directoryFile.exists && directoryFile.isDirectory ) {
      directoryFile.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }


  def isFileEmpty(file: String) = Source.fromFile(file).isEmpty


  def initTraceFile(tracePath: String, traceFileName: String, header: String): Unit = {
    val traceFilePath = tracePath + traceFileName
    if ( isFileEmpty(traceFilePath) ) {
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
    val traceFilePath = tracePath + traceFileName
    val traceFile = spark.read.option("delimiter", ";").option("header", "true").csv(traceFilePath)
    traceFile
      .select("Cheksum")
      .collect()
      .map(x => x.getString(0))

  }


  def traceWriter(traceFileWriter: FileWriter, traceFilePath: String, message: String) = {
    traceFileWriter.write(message)
  }


  // case class TracedMoveAction(file: File, sourceDirectory: String, destinationDirectory: String,traceFilePath: String,
  //   tracedFilesChecksum : Array[String])
  //  {
  def tracedMove(file: File, sourceDirectory: String, destinationDirectory: String, traceFilePath: String,
                 tracedFilesChecksum: Array[String]) = {
    val traceFileWriter = new FileWriter(new File(traceFilePath), true)
    val hash = computeHash(sourceDirectory + file.getName) //check du nveau fichier  // APPEL
    val exists = Files.exists(Paths.get(destinationDirectory + file.getName)) // true ou false existe dans le rep destination

    val length = file.length()
    val sdateformat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
    val lastmodifieddate = sdateformat.format(file.lastModified())
    // complexity is proportional to tracedFilesChecksum s length
    var traceValue: trace = null
    (exists, tracedFilesChecksum.contains(hash)) match {
      case (false, false) => {

        Files.move(
          Paths.get(sourceDirectory + file.getName),
          Paths.get(destinationDirectory + file.getName))
        val messageSuccess: String = file.getName + ";" + file.getAbsolutePath + ";" + destinationDirectory + file.getName + ";MOVE SUCCESS: File's Name  dosen't exist yet !" + ";" + hash + ";Cheksum dosen't exist yet !" + String.format("%n")
        traceWriter(traceFileWriter, traceFilePath, messageSuccess)
        traceValue = trace(file.getName, file.getAbsolutePath, destinationDirectory + file.getName, "MOVE SUCCESS: File's Name  dosen't exist yet !", hash, "Cheksum dosen't exist yet !", length.toString, lastmodifieddate)
      }

      case (true, false) => {

        val messageFileNameExists = file.getName + ";" + file.getAbsolutePath + ";" + destinationDirectory + file.getName + ";MOVE FAILED: File's Name Already Exists" + ";" + hash + "; " + String.format("%n")
        traceWriter(traceFileWriter, traceFilePath, messageFileNameExists)
        traceValue = trace(file.getName, file.getAbsolutePath, destinationDirectory + file.getName, "MOVE FAILED: File's Name Already Exists", hash, "", length.toString, lastmodifieddate)

      }

      case (false, true) => {

        val messageChecksumExists = file.getName + ";" + file.getAbsolutePath + ";" + destinationDirectory + file.getName + ";MOVE FAILED" + ";" + hash + ";Cheksum Exists Already !" + String.format("%n")
        traceWriter(traceFileWriter, traceFilePath, messageChecksumExists)
        traceValue = trace(file.getName, file.getAbsolutePath, destinationDirectory + file.getName, "MOVE FAILED", hash, "Cheksum Exists Already !", length.toString, lastmodifieddate)

      }

      case (true, true) => {

        val messageChecksum_NameExists = file.getName + ";" + file.getAbsolutePath + ";" + destinationDirectory + file.getName + ";MOVE FAILED" + ";" + hash + ";Cheksum AND Name Exists Already !" + String.format("%n")
        traceWriter(traceFileWriter, traceFilePath, messageChecksum_NameExists)
        traceValue = trace(file.getName, file.getAbsolutePath, destinationDirectory + file.getName, ";MOVE FAILED", hash, "Cheksum AND Name Exists Already !", length.toString, lastmodifieddate)
      }

    }
    traceFileWriter.close()
    traceValue
  }

}
