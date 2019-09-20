
import java.io.{File, FileInputStream, FileWriter}
import java.nio.file.{Files, Paths}
import java.security.{DigestInputStream, MessageDigest}

import org.apache.spark.sql.SparkSession

import scala.io.Source


object CopyFileService {

  def tracedCopy(sourceDirectory: String, destinationDirectory: String, tracePath: String, traceFileName: String)(implicit spark: SparkSession): Unit = {
    val traceFilePath = tracePath + traceFileName
    initTraceFile(traceFilePath) // 2 APPEL
    val fileList = listFilesFrom(sourceDirectory) // 1 APPEL
    // TODO: useless rdd
    val tracedFilesChecksum = getTracedFilesChecksum(traceFilePath) // 3 APPEL
    val fileRdd = spark.sparkContext.parallelize(fileList)
    fileRdd.foreach(file =>
      tracedMove(file, sourceDirectory, destinationDirectory, traceFilePath, tracedFilesChecksum) // 4 APPEL
    )
  }

  private def listFilesFrom(directory: String): List[File] = {
    val directoryFile: File = new File(directory)
    if (directoryFile.exists && directoryFile.isDirectory) {
      directoryFile.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  private def isFileEmpty(file: String) = Source.fromFile(file).isEmpty


  private def initTraceFile(traceFilePath: String): Unit = {
    val header: String = "File;Source;Destination;State;Cheksum;Message;Size;LastModifiedDate"
    if (isFileEmpty(traceFilePath)) {
      val headerWriter = new FileWriter(new File(traceFilePath), true)
      headerWriter.write(header + String.format("%n"))
      headerWriter.close()
    }
  }


  private def computeHash(path: String): String = {
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


  private def getTracedFilesChecksum(traceFilePath: String)(implicit spark: SparkSession): Array[String] = {
    // if (!isFileEmpty(traceFilePath)){
    val traceFile = spark.read.option("delimiter", ";").option("header", "true").csv(traceFilePath)
    traceFile
      .select("Cheksum")
      .collect()
      .map(x => x.getString(0))

  }

  private def traceWriter(traceFileWriter: FileWriter, traceFilePath: String, message: String) = {
    traceFileWriter.write(message)
  }

  // case class TracedMoveAction(file: File, sourceDirectory: String, destinationDirectory: String,traceFilePath: String,
  //   tracedFilesChecksum : Array[String])
  //  {
  private def tracedMove(file: File, sourceDirectory: String, destinationDirectory: String, traceFilePath: String,
                         tracedFilesChecksum: Array[String]) = {


    val traceFileWriter = new FileWriter(new File(traceFilePath), true)
    val hash = computeHash(sourceDirectory + file.getName) //check du nveau fichier  // APPEL
    val exists = Files.exists(Paths.get(destinationDirectory + file.getName)) // true ou false existe dans le rep destination


    // complexity is proportional to tracedFilesChecksum s length

    (exists, tracedFilesChecksum.contains(hash)) match {
      case (false, false) => {

        Files.move(
          Paths.get(sourceDirectory + file.getName),
          Paths.get(destinationDirectory + file.getName))
        val messageSuccess: String = file.getName + ";" + file.getAbsolutePath + ";" + destinationDirectory + file.getName + ";MOVE SUCCESS: File's Name  dosen't exist yet !" + ";" + hash + ";Cheksum dosen't exist yet !" + String.format("%n")
        traceWriter(traceFileWriter, traceFilePath, messageSuccess)
      }

      case (true, false) => {

        val messageFileNameExists = file.getName + ";" + file.getAbsolutePath + ";" + destinationDirectory + file.getName + ";MOVE FAILED: File's Name Already Exists" + ";" + hash + "; " + String.format("%n")
        traceWriter(traceFileWriter, traceFilePath, messageFileNameExists)
      }

      case (false, true) => {

        val messageChecksumExists = file.getName + ";" + file.getAbsolutePath + ";" + destinationDirectory + file.getName + ";MOVE FAILED" + ";" + hash + ";Cheksum Exists Already !" + String.format("%n")
        traceWriter(traceFileWriter, traceFilePath, messageChecksumExists)
      }

      case (true, true) => {

        val messageChecksum_NameExists = file.getName + ";" + file.getAbsolutePath + ";" + destinationDirectory + file.getName + ";MOVE FAILED" + ";" + hash + ";Cheksum AND Name Exists Already !" + String.format("%n")
        traceWriter(traceFileWriter, traceFilePath, messageChecksum_NameExists)
      }
    }
    traceFileWriter.close()

  }

}
