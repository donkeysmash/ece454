import org.apache.spark.{SparkContext, SparkConf}

object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val output = textFile.map(line => {
      val splited = line.split(",")
      val title = splited(0)
      val ratings = splited.drop(1)
      val max = ratings.max.toInt
      ratings.zipWithIndex.filter(x => {
        try {
          x._1.toInt == max
        } catch {
          case e: Exception => false
        }
      }).foldLeft(title)((acc, rating) => acc + "," + (rating._2 + 1))
    })
       
    output.saveAsTextFile(args(1))
  }
}
