import org.apache.spark.{SparkContext, SparkConf}

object Task2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.map(x => x);
    
    output.saveAsTextFile(args(1))
  }
}