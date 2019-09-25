
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import scala.io.Source


object ArgFileConf {

  def loadConfig(configPath: String): Config = {

    val text= Source.fromFile(configPath).mkString
    val yaml = new Yaml(new Constructor(classOf[ArgParam]))

    val traceMethod = yaml.load(text).asInstanceOf[ArgParam].traceMethod
    val sourceDirectory = yaml.load(text).asInstanceOf[ArgParam].sourceDirectory
    val destinationDirectory = yaml.load(text).asInstanceOf[ArgParam].destinationDirectory
    val tracePath = yaml.load(text).asInstanceOf[ArgParam].tracePath
    val traceFileName = yaml.load(text).asInstanceOf[ArgParam].traceFileName
    val schemaFile = yaml.load(text).asInstanceOf[ArgParam].schemaFile
    val resultFile = yaml.load(text).asInstanceOf[ArgParam].resultFile
    val readMode = yaml.load(text).asInstanceOf[ArgParam].readMode
    val partitionColumn = yaml.load(text).asInstanceOf[ArgParam].partitionColumn
    val sourceFileFormat = yaml.load(text).asInstanceOf[ArgParam].sourceFileFormat


   val Conf=  Config(sourceDirectory,
                      destinationDirectory,
                      tracePath,
                      traceFileName,
                      schemaFile,
                      resultFile,
                      readMode,
                      partitionColumn,
                      sourceFileFormat ,
                      traceMethod
                     )
    Conf
    }
}
