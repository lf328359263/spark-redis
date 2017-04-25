package com.tuyoo.main

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import com.tuyoo.kafka.WeKafka
import org.apache.hadoop.hdfs.server.common.Storage
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.SparkContext
import redis.clients.jedis.Pipeline
import java.util.Date
import com.tuyoo.action.Action3

object Main extends Serializable {

  def main(args: Array[String]): Unit = {
    var group = "test"
    val prop = System.getProperties()
    val osName = prop.getProperty("os.name")
    var sparkConf: SparkConf = null
    var interval = 1000
    var maxRate = "10000"
    //var topics = "bilog_login_10,bilog_game_10"
    var topics = "bilog_game_10"
    //var topics = "bilog_login_10,bidata_game"
    var appName = ""
    var dir="ourKafka"
    var configPath=""
    if (osName.equals("Windows 7")) {
      sparkConf = new SparkConf().setMaster("local[4]").setAppName("ourKafka")
      configPath ="c:/zz/fileFromMysql_10.txt"
    } else {

      if (args.length == 7) {
        interval = args(0).toInt
        group = args(1)
        maxRate = args(2)
        topics = args(3)
        appName = args(4)
        dir=args(5)
        configPath=args(6)
        sparkConf = new SparkConf().setAppName(appName)
      } else {
        println("args 参数错误: args0 是 批处理时间间隔（int），args1 是kafka的组（string），args2 是接收kafka event的速率（int），arge3 "+
                "是kafka的topics（以逗号分隔），args4 是spark程序 名称（string）args5:checkpoint 存储位置 ,args6:client配置文件位置")
        return
      }
    }
    val arg = Array("10.3.0.49,10.3.0.50,10.3.0.51", group, topics, "1")
    sparkConf
      .set("redis.host", "HY-10-3-0-54")
      // initial redis port
      .set("redis.port", "6379")
      // optional redis AUTH password
      .set("redis.auth", "tuyougame")
      .set("redis.timeout", "8000")
      .set("spark.streaming.receiver.maxRate", maxRate)

    def createStreamingContext(): StreamingContext = {
      val sc = new SparkContext(sparkConf)
      val ssc = new StreamingContext(sc, Milliseconds(interval))
      val lines = new WeKafka().getKafkaDStream(arg, ssc)
  
   
      new Action3(ssc.sparkContext).action(lines,configPath)
      ssc.checkpoint(dir)
      ssc
    }
    val ssc = StreamingContext.getOrCreate(dir, createStreamingContext _)
    ssc.start()
    ssc.awaitTermination()

  }

}