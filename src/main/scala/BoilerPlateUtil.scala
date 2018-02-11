import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object BoilerPlateUtil {
  def readInputFile(filePath : String, sc: SparkContext): RDD[String] = {
    return sc.textFile(filePath)
  }

  def prepareSparkContext(masterHost: String, numberOfTasks: String): SparkContext =  {
    val conf = new SparkConf().setAppName("Word Counter").setMaster(s"$masterHost[$numberOfTasks]")
    return new SparkContext(conf)
  }
}
