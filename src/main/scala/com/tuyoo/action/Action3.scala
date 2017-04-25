package com.tuyoo.action

import java.util.Date
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.SparkContext
import com.redislabs.provider.redis._
import org.apache.hadoop.hdfs.server.common.Storage
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import redis.clients.jedis.Jedis
import redis.clients.jedis.Pipeline
import org.apache.spark.rdd.RDD

class Action3(sc: SparkContext) extends Serializable {

  //所需维度
  //  val gameList = Array(9, 11, 8, 12, 14, 15) //platform product game channel channel_nick client
  //  val loginList = Array(10, 12, 13, 15, 16) //platform product channel channel_nick client
  //  
  //  val gamePre = Array("100000", "110000", "111000", "111100", "111110", "111111", "101000")
  //  val pre = Array("10000", "11000", "11100", "11110", "11111", "11111")
  //val date = new Date()
  def action(lines: DStream[(String, String)], configPath: String)(implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {
    val keyByRecType = lines.map(x => {
      val yy = x._2.replaceAll("\u0000", "")
      yy.split("\t")
    }).cache()
    //登录11000 11001 11002 11003 注册 11008 11009 11010 11014    

    val loginKeyByEventId = keyByRecType.filter(x => x(2).equals("2") && (x(6).equals("11000") || x(6).equals("11001") || x(6).equals("11002") || x(6).equals("11003")
      || x(6).equals("11008") || x(6).equals("11009") || x(6).equals("11010") || x(6).equals("11014"))).cache
    //    //client_id 16 product_nickname 15 product_version 14 channel_id 13
    addData(loginKeyByEventId, configPath, 16, "login", redisConfig)
    //game client_id 位置 15
    val gameKeyByEventId = keyByRecType.filter(x => x(2).equals("4") && x(6).equals("14001")).cache

    addData(gameKeyByEventId, configPath, 15, "game", redisConfig)
    //    action2(loginKeyByEventId, pre, Array(""), loginList, "enter\tlogin", "7\t7", "6:\t1:", date)
    //    action2(gameKeyByEventId, gamePre, Array(""), gameList, "newUsersCount", "7", "2:", date)
  }

  def addData(lines: DStream[Array[String]], configPath: String, clientPage: Int, log: String, redisConfig: RedisConfig) {
    lines.foreachRDD(x => {
      val sc = x.sparkContext
      //sc.broadcast(redisConfig)
      //  x.foreach { println }
      if (x.isEmpty()) {
        println("hello")
      } else {
        val myConf = sc.textFile(configPath)
        val confs = myConf.map(x => x.split("\t")).map(x => (x(0), x))
        val data = x.map(x => (x(clientPage), x))
        val h = data.join(confs)
        var config: Map[Int, Int] = Map()
        var list: Array[Int] = Array()
        var pre: Array[String] = Array()
        var focus = ""
        var codes = ""
        var modes = ""
        var sub = Array("")
        if (log.equals("login")) {
          config = Map(11 -> 3, 12 -> 1, 13 -> 4, 14 -> 5, 15 -> 6)
          focus = "7\t7"
          codes = "6:\t1:"
          modes = "enter\tlogin"
          list = Array(10, 12, 13, 15, 16)
          pre = Array("10000", "11000", "11100", "11110", "11111", "11111")
        } else {
          config = Map(10 -> 3, 11 -> 1, 12 -> 4, 13 -> 5, 14 -> 6)
          focus = "7"
          codes = "2:"
          modes = "newUsersCount"
          list = Array(9, 11, 8, 12, 14, 15)
          pre = Array("100000", "110000", "111000", "111100", "111110", "111111", "101000")
        }
        val ByRecType = h.map(x => {
          val line = x._2._1
          var b = line
          val conf = x._2._2
          config.foreach(x => {
            b(x._1) = conf(x._2)
          })
          ((b(3) + "000").toLong, b)
        })
        val foc = focus.split("\t")
        val code = codes.split("\t")
        val len = foc.length
        val mode = modes.split("\t")
        val rp = new RedisProxy
        ByRecType.foreachPartition(x => {
          val conn = redisConfig.connectionForKey("HY-10-3-0-54")
          val pipeline = conn.pipelined
          x.foreach(x => {
            for (i <- 0 until len) {
              val f = foc(i).split(",")
              var value = x._2(f(0).toInt)
              for (ii <- 1 until f.length) {
                value += (":" + x._2(f(ii).toInt))
              }
              val date = new Date()
              var db = 0L
              date.setTime(x._1)
              db = String.format("%td", date).toLong
              if (conn.getDB != db) {
                pipeline.select(db.toInt)
              }
              pre.foreach(p => sub.foreach(s => {
                var key = code(i)
                val codeSub = p + s
                key += codeSub
                val cd = codeSub.toCharArray()
                for (j <- 0 until cd.length) {
                  key = if (cd(j) == '1') key + "_" + x._2(list(j)) else key + "_-1"
                }
                mode(i) match {
                  case "enter" =>
                    if (x._2(6).equals("11000") || x._2(6).equals("11001") || x._2(6).equals("11002") || x._2(6).equals("11003")) {
                      //println("key:" + key + ",value:" + value)
                      pipeline.sadd(key, value)
                      rp.inputRedis(pipeline, key, value, null, db.toInt, 2)
                    }
                  case "login" =>
                    if (x._2(6).equals("11008") || x._2(6).equals("11009") || x._2(6).equals("11010") || x._2(6).equals("11014")) {
                      //println("key:" + key + ",value:" + value)
                      pipeline.sadd(key, value)
                      rp.inputRedis(pipeline, key, value, null, db.toInt, 2)
                    }
                  case "newUsersCount" =>
                    //println("key:" + key + ",value:" + value)
                    pipeline.sadd(key, value)
                    rp.inputRedis(pipeline, key, value, null, db.toInt, 2)
                }
              }))
            }
          })
          pipeline.sync
          conn.close
        })
      }

    })

  }

}