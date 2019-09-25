import scala.beans.BeanProperty

class ArgParam {
  @BeanProperty var traceMethod: String = null
  @BeanProperty var destinationDirectory: String = null
  @BeanProperty var tracePath: String = null
  @BeanProperty var traceFileName: String = null
  @BeanProperty var schemaFile: String = null
  @BeanProperty var resultFile:  String = null
  @BeanProperty var readMode: String = null
  @BeanProperty var partitionColumn :  String = null
  @BeanProperty var sourceDirectory :  String = null
  @BeanProperty var sourceFileFormat :  String = null
}
