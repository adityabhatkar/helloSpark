import org.apache.spark.{SparkConf, SparkContext}

object HelloSpark {

  def main(args: Array[String]): Unit = {
    val friendsFileName = "/home/aditya/SparkCourse/fakefriends.csv"
    val conf = new SparkConf().setAppName("Friends By Age").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val friends = sc.textFile(friendsFileName, 2).cache()
    val ageAndFriends = friends.map(line => parseLine(line)).mapValues(friends => (friends,1))
    val avg = ageAndFriends.reduceByKey((a, b) => add(a, b)).mapValues(a => a._1 / a._2).sortByKey().collect()
    for (a <- avg) {
      println(a._1 + " " + a._2)
    }
    sc.stop()
  }

  def add(pair1: (Int, Int), pair2: (Int, Int)) : (Int, Int) = {
    return (pair1._1 + pair2._1, pair1._2 + pair2._2)
  }

  def parseLine(line: String): (Int, Int) = {
    val age = line.split(",")(2).toInt
    val numberOfFriends = line.split(",")(3).toInt
    return (age, numberOfFriends)
  }
}
