object WordCounter {

  def main(args: Array[String]): Unit = {
    val sc = BoilerPlateUtil.prepareSparkContext(args(1), args(2))
    val book = BoilerPlateUtil.readInputFile(args(0), sc)
    val wordCount = book.flatMap(line => line.split(" ")).countByValue()
    for((word, count) <- wordCount) {
      val cleanWord = word.replaceAll("[^\\x20-\\x7e]", "")
      println(s"${cleanWord} ${count}")
    }
  }
}
