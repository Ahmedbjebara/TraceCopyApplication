


import java.io.{File, FileInputStream}
import java.nio.file.{Files, Paths}
import java.security.{DigestInputStream, MessageDigest}
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession


object CopyKafkaTopicService {



  def tabletracedCopy(sourceDirectory: String, destinationDirectory: String)(implicit spark: SparkSession) = {
    import spark.implicits._

   initHiveTable()


    val fileList = listFilesFrom(sourceDirectory)
    // TODO: useless rdd
    val fileRdd = spark.sparkContext.parallelize(fileList)
    val tracedFilesChecksum = getTracedFilesChecksum()

    val dataFramey =fileRdd.map(file => {
      tracedMove(file, sourceDirectory, destinationDirectory, tracedFilesChecksum )
    }).toDF()

    dataFramey.write.mode("append").insertInto("TRACETABLE")
  }


  def listFilesFrom(directory: String): List[File] = {
    val directoryFile: File = new File(directory)
    if ( directoryFile.exists && directoryFile.isDirectory ) {
      directoryFile.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }





  def initHiveTable( )(implicit spark: SparkSession)  = {
    spark.sql("CREATE TABLE IF NOT EXISTS TRACETABLE(File  STRING, Source STRING , Destination STRING , State STRING , Cheksum STRING , Message STRING , Size STRING ,LastModifiedDate STRING)")
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


  def getTracedFilesChecksum( )(implicit spark: SparkSession): Array[String] = {
   spark.sqlContext.sql("SELECT Cheksum FROM TRACETABLE").collect()
      .map(x => x.getString(0))
  }



  // case class TracedMoveAction(file: File, sourceDirectory: String, destinationDirectory: String,traceFilePath: String,
  //   tracedFilesChecksum : Array[String])
  //  {
  def tracedMove(file: File, sourceDirectory: String, destinationDirectory: String, tracedFilesChecksum: Array[String]) = {

    val hash = computeHash(sourceDirectory + file.getName) //check du nveau fichier  // APPEL
    val exists = Files.exists(Paths.get(destinationDirectory + file.getName)) // true ou false existe dans le rep destination

    val length = file.length()
    val sdateformat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
    val lastmodifieddate = sdateformat.format(file.lastModified())
    // complexity is proportional to tracedFilesChecksum s length
    var traceValue: Trace = null
    (exists, tracedFilesChecksum.contains(hash)) match {
      case (false, false) => {

        Files.move(
          Paths.get(sourceDirectory + file.getName),
          Paths.get(destinationDirectory + file.getName))
        traceValue = Trace(file.getName, file.getAbsolutePath, destinationDirectory + file.getName, "MOVE SUCCESS: File's Name  dosen't exist yet !", hash, "Cheksum dosen't exist yet !", length.toString, lastmodifieddate)
      }

      case (true, false) => {

        val messageFileNameExists = file.getName + ";" + file.getAbsolutePath + ";" + destinationDirectory + file.getName + ";MOVE FAILED: File's Name Already Exists" + ";" + hash + "; " + String.format("%n")
        traceValue = Trace(file.getName, file.getAbsolutePath, destinationDirectory + file.getName, "MOVE FAILED: File's Name Already Exists", hash, "", length.toString, lastmodifieddate)

      }

      case (false, true) => {

        val messageChecksumExists = file.getName + ";" + file.getAbsolutePath + ";" + destinationDirectory + file.getName + ";MOVE FAILED" + ";" + hash + ";Cheksum Exists Already !" + String.format("%n")
        traceValue = Trace(file.getName, file.getAbsolutePath, destinationDirectory + file.getName, "MOVE FAILED", hash, "Cheksum Exists Already !", length.toString, lastmodifieddate)

      }

      case (true, true) => {

        val messageChecksum_NameExists = file.getName + ";" + file.getAbsolutePath + ";" + destinationDirectory + file.getName + ";MOVE FAILED" + ";" + hash + ";Cheksum AND Name Exists Already !" + String.format("%n")

        traceValue = Trace(file.getName, file.getAbsolutePath, destinationDirectory + file.getName, "MOVE FAILED", hash, "Cheksum AND Name Exists Already !", length.toString, lastmodifieddate)
      }

    }
    traceValue
  }


}
