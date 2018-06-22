import org.apache.spark.{SparkContext, SparkConf}

object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)
    val idToTitle = collection.mutable.Map[Long, String]()
    val titleToId = collection.mutable.Map[String, Long]()
    val textFile = sc.textFile(args(0)).map(line => line.split(",")).persist()
    val titles = textFile.map(x => x(0)).sortBy(_(0)).zipWithIndex.collect()
    for (title <- titles) {
      idToTitle(title._2) = title._1
      titleToId(title._1) = title._2
    }
    val idToTitleBC = sc.broadcast(idToTitle)
    val titleToIdBC = sc.broadcast(titleToId)


    val output = textFile.map(splited => {
        val titleId = titleToIdBC.value(splited(0))
        (titleId, splited.tail)
      })
      .flatMap(x => {
        val title = x._1
        x._2.zipWithIndex.map(rating => {
          val userid = rating._2 + 1
          (userid.toString(), (title, if (rating._1.length() > 0) rating._1.toInt else 0))
        })
      })
      .filter(x => x._2._2 > 0)
      .persist()

    val output2 = output.join(output)
      .filter(x => x._2._1._1 < x._2._2._1 && x._2._1._2 == x._2._2._2)
      .map(x => {
        val key = (x._2._1._1, x._2._2._1)
        (key, 1)
      })
      .reduceByKey(_ + _)
      .map(x => idToTitleBC.value(x._1._1) + "," + idToTitleBC.value(x._1._2) + "," + x._2)

    output2.saveAsTextFile(args(1))
  }
}

