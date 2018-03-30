var list1 : Set[Set[Int]] = Set(Set(1,5),Set(2,4),Set(1,4))
var rdd1 = list1.map( wordSet => list1.map(wordSet1 => wordSet & wordSet1))
//    val Uid_SetMid =  rawRatings.map(line => (line._1._1, (line._1._2))).groupByKey().sortByKey().map(line => (line._1, line._2.toSet))
//    val MidList = Uid_SetMid.map(line => line._2).toArray().toList //去掉uid
//    //Uid_SetMid.foreach(x => println(x)) //顺序是按照UID排列的
//    //MidList.foreach(x => println(x))
//    val coRatedMovie = MidList.map( line => MidList.map(line1 => line & line1))
//    //coRatedMovie.foreach(x => {x.foreach(y => print(y));println("\r")})
//    coRatedMovie.foreach(x => println(x))

//    val Uid_SetMid =  rawRatings.map(line => (line._1._1, (line._1._2))).groupByKey().sortByKey().map(line => (line._1, line._2.toSet))
//    val MidList = Uid_SetMid.map(line => line._2).toArray().toList
//    MidList.foreach(x => println(x)) //顺序是按照UID排列的
//    def getCoRatedItem(user: Set[Set[Int]]): Set[List[Set[Int]]] = {
//      val coRatedMovie = user.map( line => MidList.map(line1 => line & line1))
//      coRatedMovie
//    }
//    val Chang_Fan_task2 = getCoRatedItem(Set(Set(1,2)))
//    Chang_Fan_task2.foreach(x => {x.foreach(y => print(y));println("\r")})

//    val UidToPre = testData.map(x => x._2).distinct().map(x => (x, null))
//    UidToPre.foreach(x => println(x))
//    val U_M_R = rawRatings.map(x => (x._1._2, (x._1._1, x._2)))
//    U_M_R.foreach(x => println(x))
//    val UidTP_ItsRates = UidToPre.join(U_M_R).map(x => (x._1, (x._2._2._1, x._2._2._2)))
//    UidTP_ItsRates.foreach(x => println(x))

//    def getUsersMovies(user: (Int, Int)): RDD[Int] = {
//      println("run get！")
//      val OneUid = sc.makeRDD(List(user._1))
//      OneUid.foreach(x => println(x))
//      OneUid
//    }
//    val Chang_Fan_task2 = testData.map(coRatedMovie)
//    val temp = testData.take(1)
//    temp.foreach(x => getUsersMovies(x))
//    val num = ratings.map(x => (x._1._1, 1)).reduceByKey(_ + _)
//    val sum = ratings.map(x => (x._1._1, x._2)).reduceByKey(_ + _)
//    val sum_num = sum.join(num)
//    val avgOfUsers = sum_num.map(x => (x._1, x._2._1 / x._2._2))
//    val normalizedRatings = ratings.map(x => (x._1._1, (x._1._2, x._2))).join(avgOfUsers)
//      .map(x => ((x._1, x._2._1._1), x._2._1._2 - x._2._2)).sortByKey() // ((uid,mid),rate)
//    //normalizedRatings.foreach(x => println(x))

//    val uidToPre = testData.map(x => (x._1)).distinct()
//    val uidAll = rawRatings.map(x => (x._1._1)).distinct()
//    val uidComb = uidToPre.cartesian(uidAll).sortBy(x => x)
//    uidComb.foreach(x => println(x))

//    def findCoRateUser(toPre : (Int, Int)) : RDD[Int] = {
//      printf("input point is %d, %d \n", toPre._1, toPre._2)
//      val user = M_UR.filter(x => x._1 == toPre._2).map(x => x._2._1).sortBy(x => x)
//      user.foreach(x => println(x))
//      user
//    }
//    testData.collect().foreach(x => findCoRateUser(x))

//    def Chang_Fan_task2(pred:(Int, Int)) : Unit = {
//      //printf("User %d and User %d ! \n", pred._1, pred._2)
//      val self = ratings.filter(x => x._1._1 == pred._1).sortByKey()
//      //self.foreach(x => println(x))
//      val other = ratings.filter(x => x._1._1 == pred._2).sortByKey()
//      //other.foreach(x => println(x))
//      val comb = self.union(other) // ((uid,mid),rate)
//      //comb.foreach(x => println(x))
//      val comb_midUIDRATE = comb.map(x => (x._1._2, (x._1._1, x._2))).sortByKey() // (mid,(uid,rate))
//      //comb_midUIDRATE.foreach(x => println(x))
//      val mid_itsNum = comb_midUIDRATE.map(x => (x._1, 1)).reduceByKey(_ + _) // (mid,#)
//      //mid_itsNum.foreach(x => println(x))
//      val coRatedMid = mid_itsNum.filter(x => x._2 == 2).map(x => (x._1, null))
//      //coRatedMid.foreach(x => println(x))
//      val temp = coRatedMid.join(comb_midUIDRATE).map(x => (x._1, x._2._2))
//      //temp.foreach(x => println(x))
//    }
//    uidComb.collect().foreach(x => Chang_Fan_task2(x))




import java.io.{File, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Set

object test {
  val writer = new FileWriter(new File("Chang_Fan_result_task2.txt"))

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Chang_Fan_HW3_task2").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("ratings.csv")
    val header = lines.first()
    val data = lines.filter(line => line != header)
    val rawRatings = data.map(line => ((line.split(",")(0).toInt, line.split(",")(1).toInt), line.split(",")(2).toDouble)).sortByKey() //((UID,MID),Rate)
    val testLines = sc.textFile("testing_small.csv")
    val header2 = testLines.first()
    val testData = testLines.filter(line => line != header2).map(line => (line.split(",")(0).toInt, line.split(",")(1).toInt)).sortByKey()
    val testData0 = testData.map(line => ((line._1, line._2), 0)) //UID,MID,0
    val ratings = rawRatings.subtractByKey(testData0)
      .map(line => ((line._1._1.toInt, line._1._2.toInt), line._2.toDouble)) // Rating((UID,MIN),Rate)
    //ratings.foreach(x => println(x))

    val Dm_DuR = ratings.map(x => (x._1._2, (x._1._1, x._2)))//.sortByKey()
    //Dm_DuR.foreach(x => println(x)) // (mid,(uid,rate))
    val joinSelf = Dm_DuR.join(Dm_DuR)//.sortByKey()//.filter(x => x._2._1 != x._2._2)
    //joinSelf.foreach(x => println(x))
    //val allRateComb = joinSelf.map(x => ((x._1, x._2._1._1, x._2._2._1), (x._2._1._2, x._2._2._2))).sortByKey()
    //  .filter(x => x._1._2 != x._1._3) // ((mid,user1,user2),(rate1, rate2))存在重复，例如AB,BA
    //allRateComb.foreach(x => println(x))

    val Pm_Pu = testData.map(x => (x._2, x._1))
    val PmPu_DuR = Pm_Pu.join(Dm_DuR) // (P_mid(P_uid,(DataUid,rate)))
      .map(x => ((x._1, x._2._1), (x._2._2._1, x._2._2._2)))//.sortByKey() //（(要预测的m,u),(数据集中评过此m的u)）
    //PmPu_DuR.foreach(x => println(x))
    val pairToCal = PmPu_DuR.map(x => (x._1._2, x._2._1)) // (user1,user2)需要计算w的用户对
      .map(x => (x, null))
    //pairToCal.foreach(x => println(x))
    val temp1 = joinSelf.map(x => ((x._1, x._2._1._1, x._2._2._1), (x._2._1._2, x._2._2._2))).sortByKey()
      .filter(x => x._1._2 != x._1._3)
      .map(x => ((x._1._2, x._1._3), (x._1._1, x._2._1, x._2._2))) //所有的((user1,user2),(mid,rate1,rate2))
    //temp1.foreach(x => println(x))
    val temp2 = pairToCal.join(temp1)//.sortByKey()//.map(x => ((x._1._1, x._1._2, x._2._2._1),(x._2._2._2, x._2._2._3)))
      .map(x => (x._1, x._2._2)) //预测所需的((user1,user2),(mid,rate1,rate2))即((要计算的用户对),(共有的mid，两者评分))
    //temp2.foreach(x => println(x))
    val num = temp2.map(x => (x._1, 1)).reduceByKey(_ + _)
    //number.foreach(x => println(x))
    val sum =  temp2.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
    //summation.foreach(x => println(x))
    val S_N = sum.join(num)
    //S_N.foreach(x => println(x))
    val average = S_N.map(x => (x._1, (x._2._1._2 / x._2._2, x._2._1._3 / x._2._2))) // ((user1,user2),(co-avg1,co-avg2))
    //average.foreach(x => println(x))
    //val temp3 = temp2.join(average) // ((user1,user2),((mid,rate1,rate2),(co-avg1,co-avg2)))
    //temp3.foreach(x => println(x))
    val normalizedRatings = temp2.join(average).map(x =>(x._1, x._2._1._1, x._2._1._2 - x._2._2._1, x._2._1._3 - x._2._2._2))
    //normalizedRatings.foreach(x => println(x)) //减去共有均值后的((user1,user2),mid,rate1,rate2)
    //val temp4 = normalizedRatings.map(x => (x._1, (x._2, x._3 * x._4))) // ((user1,user2),(mid,product of this mid))
    //temp4.foreach(x => println(x))
    //val number = normalizedRatings.map(x => (x._1, (x._2, x._3 * x._4)))
    //  .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)) // ((user1,user2),(sum_mid,number))
    //number.foreach(x => println(x))
    //val temp5 = normalizedRatings.map(x => (x._1, (x._2, x._3 * x._3, x._4 * x._4))) // ((user1,user2),(mid,n_rate1^2,n_rate2^2))
    //val temp6 = normalizedRatings.map(x => (x._1, (x._2, x._3 * x._3, x._4 * x._4)))
    // .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3)) //((user1,user2),(sum of n_rate1^2,sum of n_rate2^2))
    val denominator = normalizedRatings.map(x => (x._1, (x._2, x._3 * x._3, x._4 * x._4)))
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
      .map(x => (x._1, Math.sqrt(x._2._2) * Math.sqrt(x._2._3))) // ((user1,user2),denominator)
    //denominator.foreach(x => println(x))
    val pearson = normalizedRatings.map(x => (x._1, (x._2, x._3 * x._4)))
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
      .join(denominator).map(x => (x._1, x._2._1._2 / x._2._2)) // ((user1,user2),correlation coefficient)
    //pearson.foreach(x => println(x))
    val temp7 = pearson.filter(x => x._2.isNaN).map(x => (x._1, 0.0))
    //temp7.foreach(x => println(x))
    val W = pearson.subtractByKey(temp7).union(temp7)//.sortByKey() // 把NaN替换为0 ((user1,user2),cc)
    W.foreach(x => println(x))

    //    val Du_DmR = ratings.map(x => (x._1._1, (x._1._2, x._2)))//.sortByKey()
    //    //Du_DmR.foreach(x => println(x))
    //    val PuPm_DmR = testData.join(Du_DmR).map(x => ((x._1, x._2._1),(x._2._2._1, x._2._2._2))) // ((预测点),(点对应的用户的所有评分))
    //    //PuPm_DmR.foreach(x => println(x))
    //
    //    val num2 = PuPm_DmR.map(x => (x._1, 1)).reduceByKey(_ + _)
    //    val sum2 = PuPm_DmR.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
    //    val avg2 = sum2.join(num2).map(x => (x._1._1, x._2._1._2 / x._2._2)).distinct() //(uid,avg)所有要预测用户自身的平均分（公式第一项）
    //    //avg2.foreach(x => println(x))
    //
    //    //PmPu_DuR.foreach(x => println(x))
    //    val temp8 = PmPu_DuR.map(x => (x._1._2, (x._1._1, x._1._2, x._2._1))) // (预测点u,(m,涉及的pairs))
    //    //temp8.foreach(x => println(x))
    //    val temp9 = temp8.join(avg2)  // (预测点u,((m,涉及的pairs),要预测的u的均值))
    //      .map(x => ((x._2._1._2, x._2._1._3), (x._1, x._2._1._1, x._2._2))) //((涉及的pair)(预测点,要预测的u的均值))
    //    //temp9.foreach(x => println(x))
    //    val temp10 = temp9.join(W) // ((涉及的pair),((预测点，均值),此pair的W))
    //      .map(x => ((x._1._2, x._2._1._2),(x._1._1, x._2._1._1, x._2._1._3, x._2._2))) // ((涉及的对方,预测的m),(涉及的自己,预测的u,要预测u的均分,此pair的W))
    //    //temp10.foreach(x => println(x))
    //    val temp11 = temp10.join(ratings) //加入评分
    //      .map(x => ((x._1._1, x._1._2), (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._2))) //((涉及的对方,预测的m)，(涉及的自己,预测的u,要预测u的均分ra,此pair的W,对方的评分ru))
    //    //temp11.foreach(x => println(x))
    //
    //    val other_PreM = temp8.map(x => (x._2._3, x._2._1)) // (对方，m) m代表需要去掉的项
    //    //other_PreM.foreach(x => println(x))
    //    val temp12 = ratings.map(x => (x._1._1, (x._1._2, x._2)))
    //    val temp13 = other_PreM.join(temp12) //(对方,(m,(对方评过的M，分数))
    //      .map(x => ((x._1, x._2._2._1, x._2._1), x._2._2._2)) // (对方,对方评过的M,m{即要去掉的项},分数)
    //      .filter(x => x._1._2 != x._1._3) //(对方,对方评过的M,此数据用于预测m,分数)
    //      .map(x => ((x._1._1, x._1._3), (x._1._2, x._2))) //(对方，此数据用于预测m，对方评过的M，分数)
    //    //temp13.foreach(x => println(x))
    //    //val temp14 =
    //    val num3 = temp13.map(x => (x._1, 1)).reduceByKey(_ + _)
    //    val sum3 = temp13.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
    //    val avg3 = sum3.join(num3).map(x => (x._1, x._2._1._2 / x._2._2))
    //    //avg3.foreach(x => println(x))
    //    val temp14 = temp11.join(avg3) //((涉及的对方,预测的m)，((涉及的自己,预测的u,要预测u的均分ra,此pair的W,对方的评分ru),对方平均值ru-))
    //      .map(x => ((x._2._1._2, x._1._2, x._2._1._3), (x._2._1._1, x._1._1, x._2._1._4, x._2._1._5 - x._2._2))) // ((u,m,要预测u的均分ra),(涉及的自己,涉及的对方,此pair的W,对方的评分ru - 对方平均值ru))
    //    //temp14.foreach(x => println(x))
    //    val temp15 = temp14.map(x => ((x._1._1, x._1._2, x._1._3), (x._2._4 * x._2._3, x._2._3)))
    //    //temp15.foreach(x => println(x))
    //    val temp16 = temp15.reduceByKey((x,y) => (x._1 + y._1, Math.abs(x._2) + Math.abs(y._2)))
    //    //temp16.foreach(x => println(x))
    //    val result = temp16.map(x => ((x._1._1, x._1._2), x._1._3 + x._2._1 / x._2._2))
    //    //result.foreach(x => println(x))
    //    val temp17 = result.join(rawRatings)
    //    temp17.foreach(x => println(x))
  }
}
