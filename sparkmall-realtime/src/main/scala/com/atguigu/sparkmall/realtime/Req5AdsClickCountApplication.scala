package com.atguigu.sparkmall.realtime

import com.atguigu.sparkmall.common.util.DateUtil
import com.atguigu.sparkmall.realtime.util.{KafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object Req5AdsClickCountApplication {

    def main(args: Array[String]): Unit = {

        // 需求五：每天各地区各城市各广告的点击流量实时统计

        // 准备SparkStreaming上下文环境对象
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req4BlackNameListApplication")

        val streamingContext = new StreamingContext(sparkConf, Seconds(5))

        streamingContext.sparkContext.setCheckpointDir("cp")

        val topic =  "ads_log"
        // TODO 从Kafka中获取数据
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream(topic, streamingContext)

        // 将获取的kafka数据转换结构
        val adsClickDStream: DStream[AdsClickKafkaMessage] = kafkaDStream.map(data => {

            val datas: Array[String] = data.value().split(" ")

            AdsClickKafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
        })

        // TODO 1. 将数据转换结构 （date-area-city-ads, 1）
        val dateAdsUserToOneDStream: DStream[(String, Long)] = adsClickDStream.map(message => {
            val date: String = DateUtil.formatStringByTimestamp(message.timestamp.toLong, "yyyy-MM-dd")
            (date + "_" + message.area + "_" + message.city + "_" + message.adid, 1L)
        })

        // TODO 2. 将转换结构后的数据进行聚合 （date-area-city-ads, sum）
        val reduceDStream: DStream[(String, Long)] = dateAdsUserToOneDStream.reduceByKey(_+_)

        // TODO 3. 更新Redis中最终统计结果
        reduceDStream.foreachRDD(rdd=>{
            rdd.foreachPartition(datas=>{
                val client: Jedis = RedisUtil.getJedisClient

                datas.foreach{
                    case ( field, sum ) =>
                        client.hincrBy("date:area:city:ads", field, sum)
                }

                client.close()
            })
        })


        // 启动采集器
        streamingContext.start()
        // Driver应该等待采集器的执行结束
        streamingContext.awaitTermination()
    }
}
