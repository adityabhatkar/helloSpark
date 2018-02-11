object WordCounter {

  def main(args: Array[String]): Unit = {
    val sc = BoilerPlateUtil.prepareSparkContext(args(1), args(2))
    val book = BoilerPlateUtil.readInputFile(args(0), sc)
    val words = book.flatMap(line => normalizeWord(line))
    val wordCount = words.map(word => (word, 1)).reduceByKey((a , b) => a + b)
    val wordCountSorted = wordCount.map(wc => wc.swap).sortByKey().collect()
    for((count, word) <- wordCountSorted) {
      val cleanWord = word.replaceAll("[^\\x20-\\x7e]", "")
      println(s"${cleanWord} ${count}")
    }
  }

  def normalizeWord(line: String): Array[String] = {
    return line.split("\\W+").map(word => word.toLowerCase())
  }
}
