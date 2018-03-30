import java.io.{File, FileWriter}

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

object Chang_Fan_task1 {
  val writer = new FileWriter(new File("Chang_Fan_result_task1.txt"))

  def main(args: Array[String]): Unit = {
    val t1 = System.currentTimeMillis()
    val conf = new SparkConf().setAppName("CollaborativeFilteringExample").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(args(0))
    val header = lines.first()
    val data = lines.filter(line => line != header)
    val rawRatings = data.map(line => ((line.split(",")(0), line.split(",")(1)), line.split(",")(2)))
    val testLines = sc.textFile(args(1))
    val header2 = testLines.first()
    val testData = testLines.filter(line => line != header2).map(line => (line.split(",")(0), line.split(",")(1)))
    val testData0 = testData.map(line => ((line._1, line._2), 0)) //UID,MID,0
    val ratings = rawRatings.subtractByKey(testData0)
      .map(line => Rating(line._1._1.toInt, line._1._2.toInt, line._2.toDouble)) // Rating(UID,MIN,Rate)

    // Build the recommendation model using ALS
    val rank = 2
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    // Evaluate the model on rating data
    val predicateData = testData0.leftOuterJoin(rawRatings)
      .map(line => (line._1._1.toInt, line._1._2.toInt, line._2._2.toString.substring(5, 8).toDouble)) // 要预测的数据(UID,MID,Rate)

    val usersProducts = predicateData.map(line => (line._1, line._2))
    var predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }.sortByKey()

//    val temp1 = predictions.filter(x => x._2 > 5).map(line => (line._1, 5.0))
//    val temp2 = predictions.subtractByKey(temp1).union(temp1)
//    val temp3 = predictions.filter(x => x._2 < 0).map(line => (line._1, 0.0))
//    val temp4 = temp2.subtractByKey(temp3).union(temp3)
//    predictions = temp4 // normalize rates out of range
    val temp = predictions.map(x => {
      val testPoint = x._1
      var predictValue = x._2
      if (predictValue.isNaN) {
        predictValue = 0
      } else if (predictValue > 5) {
        predictValue = 5
      } else if (predictValue < 0) {
        predictValue = 0
      }
      (testPoint, predictValue)
    })
    predictions = temp

    val predictUID_MID_null = usersProducts.map(x => ((x._1, x._2), null)) // 所有要预测的数据
    val intRawRatings = rawRatings.map(x => ((x._1._1.toInt, x._1._2.toInt), x._2.toDouble))
    val missingData = predictUID_MID_null.subtractByKey(predictions) //.leftOuterJoin(intRawRatings) // 丢失的数据与他们的评分

    val missingUID = missingData.map(line => line._1._1).distinct().map(line => (line, null)) // 丢失数据对应的UID
    val rawUID_Rate = intRawRatings.map(line => (line._1._1, line._2)) // 所有训练数据中UID和对应的所有评分
    val allRateOfmUID = missingUID.leftOuterJoin(rawUID_Rate) // 找出丢失UID对应的所有评分
      .map(line => (line._1, line._2._2.toString.substring(5, 8).toDouble))
    val num = allRateOfmUID.map(line => (line._1, 1)).reduceByKey(_ + _)
    val sum = allRateOfmUID.reduceByKey(_ + _)
    //.map(line => (line._1, line._2.toDouble))
    val sum_num = sum.join(num)
    val avgOfmUID = sum_num.map(line => (line._1, line._2._1 / line._2._2)) // 计算丢失UID的平均评分(UID,AVG)

    val missingUID_MID_Rate = missingData //.predictUID_MID_null.subtractByKey(predictions).sortByKey() // missing part
      .map(line => (line._1._1, line._1._2)).join(avgOfmUID) // (UID,MID) join (UID,avg)
      .map(line => ((line._1, line._2._1), line._2._2))
    //missingUID_MID_Rate.foreach(x => println(x))
    val temp5 = predictions.union(missingUID_MID_Rate).sortByKey()
    predictions = temp5

    val ratesAndPreds = predicateData.map { case (user, product, rate) =>
      ((user, product), rate)
    }.join(predictions).sortByKey()
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = r1 - r2
      err * err
    }.mean()
    val RMSE = Math.sqrt(MSE)

    val diff = ratesAndPreds.map(line => Math.abs(line._2._1 - line._2._2))
    val group1 = diff.filter(x => x >= 0 && x < 1).count()
    val group2 = diff.filter(x => x >= 1 && x < 2).count()
    val group3 = diff.filter(x => x >= 2 && x < 3).count()
    val group4 = diff.filter(x => x >= 3 && x < 4).count()
    val group5 = diff.filter(x => x >= 4).count()

    println(">=0 and <1:" + group1 + "\n"
      + ">=1 and <2:" + group2 + "\n"
      + ">=2 and <3:" + group3 + "\n"
      + ">=3 and <4:" + group4 + "\n"
      + ">=4:" + group5 + "\n"
      + "RMSE = " + RMSE)
    //println(predictions.count(), ratesAndPreds.count(), missingData.count()) // data missing

    val stringPredictions = predictions.map(x => (x._1._1, x._1._2, x._2).toString()).map(x => x.substring(1,x.length - 1))
    val t2 = System.currentTimeMillis()
    printf("Time used: %d ms.\n", t2 - t1)
    writer.write("UserId,MovieId,Pred_rating \n")
    stringPredictions.foreach(x => writer.write(x + System.lineSeparator()))
    writer.close()
  }
}