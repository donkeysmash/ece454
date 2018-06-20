import org.apache.spark.{SparkContext, SparkConf}

object Task3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 3")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.flatMap(line => line.split(",").drop(1).zipWithIndex)
      .map(x => {
        if (x._1.length() > 0) {
          (x._2 + 1, 1)
        } else {
          (x._2 + 1, 0)
        }
      })
      .reduceByKey(_ + _)
      .map(x => x._1 + "," + x._2)

    output.saveAsTextFile(args(1))
  }
}
