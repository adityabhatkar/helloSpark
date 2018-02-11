import org.apache.spark.{SparkConf, SparkContext}

object MinMaxTempFinder {

  def main(args: Array[String]): Unit = {
    val tempFileName = "/home/aditya/SparkCourse/1800.csv"
    val conf = new SparkConf().setAppName("Min Max Temp Finder").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val tempData = sc.textFile(tempFileName, 2).map(line => parseLine(line)).cache()

    val minTemps = tempData.filter(temps => temps._2._1.equalsIgnoreCase("TMIN")).map(t => (t._1, t._2._2))
    val maxTemps = tempData.filter(temps => temps._2._1.equalsIgnoreCase("TMAX")).map(t => (t._1, t._2._2))
    val minTempByStations = minTemps.reduceByKey((temp1, temp2) => math.min(temp1, temp2)).collect()
    val maxTempByStations = minTemps.reduceByKey((temp1, temp2) => math.max(temp1, temp2)).collect()

    for(stationTemp <- minTempByStations){
      println("Min temps for station " + stationTemp._1 + " was " + stationTemp._2)
    }

    for(stationTemp <- maxTempByStations){
      println("Max temps for station " + stationTemp._1 + " was " + stationTemp._2)
    }

    val merged = minTempByStations ++ maxTempByStations
    for(stationTemp <- merged){
      println("temps for station " + stationTemp._1 + " was " + stationTemp._2)
    }
  }



  def parseLine(line: String): (String, (String, Int)) = {
    val lineArr = line.split(",")
    val station = lineArr(0)
    val tempType = lineArr(2)
    val temp = lineArr(3).toInt
    return (station, (tempType, temp))
  }
}
