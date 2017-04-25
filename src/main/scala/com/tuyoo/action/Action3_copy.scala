//package com.tuyoo.action
//
//import java.util.Date
//import org.apache.spark.streaming.dstream.DStream
//import org.apache.spark.SparkContext
//import com.redislabs.provider.redis._
//import org.apache.hadoop.hdfs.server.common.Storage
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.StreamingContext
//import redis.clients.jedis.Jedis
//import redis.clients.jedis.Pipeline
//
//class Action3_copy(sc: SparkContext) extends Serializable {
//  //所需维度
//  val coinList = Array(11, 18) //platform game
//  val payList = Array(10, 12, 13, 15, 16, 20, 26) // platform product channel channel_nick client prod_id pay_type
//  val gameList = Array(9, 11, 8, 12, 14, 15) //platform product game channel channel_nick client
//  val loginList = Array(10, 12, 13, 15, 16) //platform product channel channel_nick client
//  val paySub = Array("00", "01", "10", "11")
//  val coinPre = Array("10", "11")
//  val gamePre = Array("100000", "110000", "111000", "111100", "111110", "111111", "101000")
//  val pre = Array("10000", "11000", "11100", "11110", "11111", "11111")
//  val date = new Date()
//  def action(lines: DStream[(String, String)]) {
//    val keyByRecType = lines.map(x => {
//      val yy = x._2.replaceAll("\u0000", "")
//      print(yy)
//      val y = yy.split("\t")
//      //      println(y)
//     // scala.collection.mutable.Map(y(2) -> ((y(3) + "000").toLong, y))
//      ((y(3) + "000").toLong, y)
//    }).cache()
//    //val k=keyByRecType.flatMap(x=>x.get("1"))
//    //k.foreachRDD(x=>x.foreach(x=>println(x._2(2))))
//    //登录11000 11001 11002 11003 注册 11008 11009 11010 11014   
//    val loginKeyByEventId = keyByRecType.filter(x => (x._2(2).equals("2") &&x._2(6).equals("11000") || x._2(6).equals("11001") || x._2(6).equals("11002") || x._2(6).equals("11003")
//      || x._2(6).equals("11008") || x._2(6).equals("11009") || x._2(6).equals("11010") || x._2(6).equals("11014")) && x._2(12).toInt < 10000).cache
//    val gameKeyByEventId = keyByRecType.filter(x => x._2(2).equals("4") &&x._2(6).equals("14001") && x._2(11).toInt < 10000).cache
//    //val payKeyByEventId = keyByRecType.filter(x => x._2(2).equals("3") &&x._2(6).equals("12006") && x._2(12).toInt < 10000).cache //event_id=12006
////    val coinKeyByChipType = keyByRecType.filter(x => x._2(2).equals("1")&& !x._2(21).equals("1") && !x._2(10).equals("10029") && !x._2(10).equals("10030") && !x._2(10)
////      .equals("10032") && !x._2(10).equals("10045") && x._2(13).toInt < 10000).cache//chip_type=1
//    //action2(payKeyByEventId, pre, paySub, payList, "addUp\taddUp\tsum", "7\t17\t24", "12:\t37:\t11:", date) //payUserNum |payNum|paySum
//   // action2(coinKeyByChipType, coinPre, Array(""), coinList, "sort\tnewAddSum\tconsumeSum", "6,9\t6,7\t6,7", "50:\t39:\t40:", date) //coinRank|
//    action2(loginKeyByEventId, pre, Array(""), loginList, "enter\tlogin", "7\t7", "6:\t1:", date)
//    action2(gameKeyByEventId, gamePre, Array(""), gameList, "newUsersCount", "7", "2:", date)
//  }
//  def action2(log: DStream[(Long, Array[String])], pre: Array[String], sub: Array[String], list: Array[Int],
//              modes: String, focus: String, codes: String, date: Date)(implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {
//    log.foreachRDD(rdd => {
//      val foc = focus.split("\t")
//        val code = codes.split("\t")
//        val len = foc.length
//        val mode = modes.split("\t")
//        val rp = new RedisProxy
//      rdd.foreachPartition(x => {
//        val conn = redisConfig.connectionForKey("HY-10-3-0-54")
//        val pipeline = conn.pipelined
//        x.foreach(x => {
//          for (i <- 0 until len) {
//            val f = foc(i).split(",")
//            var value = x._2(f(0).toInt)
//            for (ii <- 1 until f.length) {
//              value += (":" + x._2(f(ii).toInt))
//            }
//            var db = 0L
//            date.setTime(x._1)
//            db = String.format("%td", date).toLong
//            if (conn.getDB != db) {
//              pipeline.select(db.toInt)
//            }
//            pre.foreach(p => sub.foreach(s => {
//              var key = code(i)
//              val codeSub = p + s
//              key += codeSub
//              val cd = codeSub.toCharArray()
//              for (j <- 0 until cd.length) {
//                key = if (cd(j) == '1') key + "_" + x._2(list(j)) else key + "_-1"
//              }
//              mode(i) match {
////                case "addUp" =>
////                  pipeline.sadd(key, value)
////                  rp.inputRedis(pipeline, key, value, null, db.toInt, 2)
////                case "sum" =>
////                  pipeline.incrByFloat(key, value.toDouble)
////                  rp.inputRedis(pipeline, key, value, null, db.toInt, 1)
////                case "sort" =>
////                  val v = value.split(":")
////                  val userId = v(0)
////                  val finalChip = v(1).toDouble
////                  pipeline.zadd(key, finalChip, userId)
////                  rp.inputRedis(pipeline, key, v(1), userId, db.toInt, 3)
////                case "newAddSum" =>
////                  val v = value.split(":")
////                  val userId = v(0)
////                  val detail_chip = v(1).toDouble
////                  if (detail_chip > 0) {
////                    pipeline.zincrby(key, detail_chip, userId)
////                    rp.inputRedis(pipeline, key, v(1), userId, db.toInt, 4)
////                  }
//                case "enter" =>
//                  if (x._2(6).equals("11000") || x._2(6).equals("11001") || x._2(6).equals("11002") || x._2(6).equals("11003")) {
//                    pipeline.sadd(key, value)
//                    rp.inputRedis(pipeline, key, value, null, db.toInt, 2)
//                  }
//                case "login" =>
//                  if (x._2(6).equals("11008") || x._2(6).equals("11009") || x._2(6).equals("11010") || x._2(6).equals("11014")) {
//                    pipeline.sadd(key, value)
//                    rp.inputRedis(pipeline, key, value, null, db.toInt, 2)
//                  }
//                case "newUsersCount" =>
//                  pipeline.sadd(key, value)
//                  rp.inputRedis(pipeline, key, value, null, db.toInt, 2)
////                case "consumeSum" =>
////                  val v = value.split(":")
////                  val userId = v(0)
////                  val detail_chip = v(1).toDouble
////                  if (detail_chip < 0) {
////                    pipeline.zincrby(key, detail_chip, userId)
////                    rp.inputRedis(pipeline, key, v(1), userId, db.toInt, 4)
////                  }
//              }
//            }))
//          }
//        })
//        pipeline.sync
//        conn.close
//      })
//
//    })
//  }
//
//}