import org.apache.spark.{SparkContext, SparkConf}

object Task2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.map(line => {
      val key = ""
      val splited = line.split(",")
      val count = splited.drop(1).foldLeft(0)((x, y) => {
        if (y.length > 0) {
          x + 1
        } else {
          x
        }
      })
      (key, count)
    }).reduceByKey(_ + _).map(x => x._2)
    output.saveAsTextFile(args(1))
  }
}
