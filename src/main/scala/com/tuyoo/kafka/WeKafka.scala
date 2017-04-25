package com.tuyoo.kafka

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.hadoop.hdfs.server.common.Storage
import org.apache.spark.storage.StorageLevel

class WeKafka extends Serializable{
   def getKafkaDStream(args:Array[String],ssc: StreamingContext):DStream[(String,String)]={
   val Array(zkQuorum, group, topics, numThreads) = args
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val numInputDStreams = 10
//    val kafkaDStreams=(1 to numInputDStreams).map(_=>KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).persist(StorageLevel.MEMORY_AND_DISK).map(_._2).persist(StorageLevel.MEMORY_AND_DISK)
//    .map(_.replaceAll("\u0000", "")).persist(StorageLevel.MEMORY_AND_DISK))
//    ssc.union(kafkaDStreams)
    KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).persist(StorageLevel.MEMORY_AND_DISK)//.map(_._2).persist(StorageLevel.MEMORY_AND_DISK)
    //.map(_.replaceAll("\u0000", "")).persist(StorageLevel.MEMORY_AND_DISK)
  }
}