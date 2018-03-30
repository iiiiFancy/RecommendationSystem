import java.io.{File, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}

object Chang_Fan_task2 {
  val writer = new FileWriter(new File("Chang_Fan_result_task2.txt"))
  def main(args: Array[String]): Unit = {
    val t1 = System.currentTimeMillis()
    val conf = new SparkConf().setAppName("Chang_Fan_HW3_task2").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(args(0))
    val header = lines.first()
    val data = lines.filter(line => line != header)
    val rawRatings = data.map(line => ((line.split(",")(0).toInt, line.split(",")(1).toInt), line.split(",")(2).toDouble)) //((UID,MID),Rate)
    val testLines = sc.textFile(args(1))
    val header2 = testLines.first()
    val testData = testLines.filter(line => line != header2).map(line => (line.split(",")(0).toInt, line.split(",")(1).toInt))
    val testData0 = testData.map(line => ((line._1, line._2), 0)) //UID,MID,0
    val ratings = rawRatings.subtractByKey(testData0)
      .map(line => ((line._1._1.toInt, line._1._2.toInt), line._2.toDouble)) // Rating((UID,MID),Rate)
    //ratings.foreach(x => println(x)
    val Dm_DuR = ratings.map(x => (x._1._2, (x._1._1, x._2)))//.sortByKey()
    //Dm_DuR.foreach(x => println(x)) // (mid,(uid,rate))
    val joinSelf = Dm_DuR.join(Dm_DuR)//.sortByKey()//.filter(x => x._2._1 != x._2._2)
    //joinSelf.foreach(x => println(x))
    val allRateComb = joinSelf.map(x => ((x._2._1._1, x._2._2._1), (x._1, x._2._1._2, x._2._2._2)))
      .filter(x => x._1._1 != x._1._2)//.groupByKey() // ((user1,user2),(用到此pair的mid(公共M),rate1, rate2))存在重复，例如AB,BA
    //allRateComb.foreach(x => println(x))
    val Pm_Pu = testData.map(x => (x._2, x._1))
    val PmPu_DuR = Pm_Pu.join(Dm_DuR) // (P_mid(P_uid,(DataUid,rate))) // 第二个join找出要计算的用户对
      .map(x => ((x._1, x._2._1), (x._2._2._1, x._2._2._2)))//.sortByKey() //（(要预测的m,u1),(数据集中评过此m的u2,u2的rate)）
    //PmPu_DuR.foreach(x => println(x))
    val pairWithMid = PmPu_DuR.map(x => ((x._1._2, x._2._1),x._1._1))
    val pairToCal = PmPu_DuR.map(x => ((x._1._2, x._2._1),null)).distinct() // ((user1,user2),Pm) 需要计算w的用户对
    //.map(x => (x, null))
    //pairToCal.foreach(x => println(x))
    val neededPair = pairToCal.join(allRateComb)// 第三个从全部评分组合中过滤出需要的用户对
      .map(x => (x._1, x._2._2)).groupByKey() //找出预测所需的((user1,user2),(共有mid,rate1,rate2))即((要计算的用户对),(用到此对的mid，两者分别的评分))
    //neededPair.sortByKey().foreach(x => println(x)) //40s

    val coAvg = neededPair.map(x => {  // ((user1,user2),(coAvg1,coAvg2))
      val pair = x._1
      val value = x._2
      val coMid = value.map(x => x._1)
      val bufferSize = value.map(x => x._2).size
      val avg1= value.map(x => x._2).sum/bufferSize
      val avg2= value.map(x => x._3).sum/bufferSize
      (pair, (avg1,avg2)) // pair第一位和PreM还原预测点
    })
    //coAvg.sortByKey().foreach(x => println(x))

    val normalizedRates = neededPair.join(coAvg).map(x => {
      val pair = x._1
      val avg1 = x._2._2._1
      val avg2 = x._2._2._2
      val value = x._2._1
      val coMid = value.map(x => x._1)   //共有的电影
      val normalizedRate1_2 = value.map(x => (x._2 - avg1, x._3 - avg2))
      (pair, normalizedRate1_2) // pair第一位和PreM还原预测点
    })
    //normalizedRates.sortByKey().foreach(x => println(x))

    val pearson = normalizedRates.map(x => {
      val pair = x._1
      val rate1_2 = x._2
      val number = rate1_2.map(x => x._1 * x._2).sum
      val deno1 = rate1_2.map(x => x._1 * x._1).sum
      val deno2 = rate1_2.map(x => x._2 * x._2).sum
      val denominator = Math.sqrt(deno1) * Math.sqrt(deno2)
      var result = 0.0
      if(denominator != 0){
        result = number / denominator
      }
      (pair, result) // pair第一位和PreM还原预测点 ((user1,user2)
    })
    //pearson.foreach(x => println(x))

    val temp = ratings.map(x =>(x._1._1, (x._1._2, x._2))).groupByKey()
    //temp.foreach(x => println(x))
    val avgAllUser = temp.map(x => {
      val user = x._1
      val value = x._2
      val num = value.map(x => x._2).size
      val sum = value.map(x => x._2).sum
      (user, (sum, num))
    })
    //avgAllUser.foreach(x => println(x))

    val addR = PmPu_DuR.map(x => ((x._1._2, x._2._1), (x._1._1, x._2._2))) // 每个预测点对应的邻居((user1,user2),(movie1,rate2))
    //addR.foreach(x => println(x))
    val addW = pearson.join(addR) //((user1,user2),(W, (用于评价'm',rate2)))
    //addW.foreach(x => println(x))
    val addUser2Avg = addW.map(x => (x._1._2, (x._1._1, x._2))).join(avgAllUser) //(user2,((user1,(W,(m,rate2))),(sum,num)))
    //addUser2Avg.foreach(x => println(x))
    val calculation = addUser2Avg.map(x => {
      val user1 = x._2._1._1
      val user2 = x._1
      val m = x._2._1._2._2._1
      val W = x._2._1._2._1
      val R = x._2._1._2._2._2
      val sum = x._2._2._1
      val num = x._2._2._2
      val normalizedR = R - (sum - R) / (num - 1)
      val R_W = normalizedR * W
      ((user1, m), (W, R_W))
    })
    //calculation.sortByKey().foreach(x => println(x))
    val prediction = calculation.reduceByKey((x,y) => (Math.abs(x._1) + Math.abs(y._1), x._2 + y._2))
      .map(x => (x._1._1, (x._1._2, x._2._2 / x._2._1))).join(avgAllUser)
      .map(x => ((x._1, x._2._1._1), x._2._1._2 + x._2._2._1 / x._2._2._2))

    val predictions = prediction.map(x => {
      val testPoint = x._1
      var predictValue = x._2
      if (predictValue.isNaN) {
        predictValue = -1
      } else if (predictValue > 5) {
        predictValue = 5
      } else if (predictValue < 0) {
        predictValue = 0
      }
      (testPoint, predictValue)
    })

    val NANid = predictions.filter(x => x._2 == -1).map(x => x._1._1)
    val predictUID_MID_null = testData.map(x => ((x._1, x._2), null)) // 所有要预测的数据
    val missingData = predictUID_MID_null.subtractByKey(predictions) // 丢失的数据
    val missingUID = missingData.map(line => line._1._1).union(NANid).distinct().map(line => (line, null)) // 丢失数据对应的UID
    val allRateOfmUID = missingUID.join(avgAllUser)
    val substitution = allRateOfmUID.map(x => (x._1, x._2._2._1 / x._2._2._2 ))
    val missingUID_MID_Rate = missingData // missing part
      .map(line => (line._1._1, line._1._2)).join(substitution) // (UID,MID) join (UID,avg)
      .map(line => ((line._1, line._2._1), line._2._2))
    val revisedPrediction = predictions.union(missingUID_MID_Rate).sortByKey()
    val ratesAndPreds = rawRatings.join(revisedPrediction).sortByKey()
    //ratesAndPreds.foreach(x => println(x))
    val MSE = ratesAndPreds.map(x => (x._2._1 - x._2._2) * (x._2._1 - x._2._2)).mean()
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
    val t2 = System.currentTimeMillis()  // count end time
    //println(prediction.count(), ratesAndPreds.count(), missingData.count())
    val stringPredictions = revisedPrediction.map(x => (x._1._1, x._1._2, x._2).toString()).map(x => x.substring(1,x.length - 1))
    printf("Time used: %d ms.\n", t2 - t1)
    writer.write("UserId,MovieId,Pred_rating \n")
    stringPredictions.foreach(x => writer.write(x + System.lineSeparator()))
    writer.close()
  }
}
