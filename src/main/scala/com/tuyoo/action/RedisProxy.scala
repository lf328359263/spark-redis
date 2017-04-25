package com.tuyoo.action

import redis.clients.jedis.Pipeline
import java.util.Date

class RedisProxy extends Serializable {

  def inputRedis(pipeline: Pipeline, key: String, v1: String, v2: String, db: Int, control: Int){
    pipeline.expire(key, 2 * 24 * 60 * 60)
    pipeline.select(40)
    control match {
      case 1 => pipeline.incrByFloat(key, v1.toDouble)
      case 2 => pipeline.sadd(key, v1)
      case 3 => pipeline.zadd(key, v1.toDouble, v2)
      case 4 => pipeline.zincrby(key, v1.toDouble, v2)
    }
    pipeline.select(db)
  }

}