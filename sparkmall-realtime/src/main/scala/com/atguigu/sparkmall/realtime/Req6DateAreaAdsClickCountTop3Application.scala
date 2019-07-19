package com.atguigu.sparkmall.realtime

import com.atguigu.sparkmall.common.util.DateUtil
import com.atguigu.sparkmall.realtime.util.{KafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.JsonMethods
import redis.clients.jedis.Jedis

object Req6DateAreaAdsClickCountTop3Application {

    def main(args: Array[String]): Unit = {

        // 需求六：每天各地区top3热门广告

        // 准备SparkStreaming上下文环境对象
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req4BlackNameListApplication")

        val streamingContext = new StreamingContext(sparkConf, Seconds(5))

        streamingContext.sparkContext.setCheckpointDir("cp")

        val topic = "ads_log"
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

        // TODO 2. 将转换结构后的数据进行有状态聚合 （date-area-city-ads, sum）
        val stateDStream: DStream[(String, Long)] = dateAdsUserToOneDStream.updateStateByKey[Long] {
            (seq: Seq[Long], buffer: Option[Long]) => {
                val sum: Long = buffer.getOrElse(0L) + seq.size
                Option(sum)
            }
        }

        // TODO 3. 将聚合后的结果进行结构的转换 (date-area-ads, sum)
        val dateAreaAdsToSumDStream: DStream[(String, Long)] = stateDStream.map {
            case (key, sum) =>
                val keys: Array[String] = key.split("_")
                (keys(0) + "_" + keys(1) + "_" + keys(3), sum)
        }

        // TODO 4. 将转换结构后的数据进行聚合 (date-area-ads, totalSum)
        val dateAreaAdsToSumReduceDStream: DStream[(String, Long)] = dateAreaAdsToSumDStream.reduceByKey(_ + _)

        // TODO 5. 将聚合后的结果进行结构的转换 (date-area-ads, totalSum) ==> (date-area,(ads,totalSum))
        val dateAreaToAdsTotalSumDStream: DStream[(String, (String, Long))] = dateAreaAdsToSumReduceDStream.map {
            case (key, totalSum) =>
                val keys: Array[String] = key.split("_")

                (keys(0) + "_" + keys(1), (keys(2), totalSum))
        }


        // TODO 6. 将数据进行分组
        val groupDStream: DStream[(String, Iterable[(String, Long)])] = dateAreaToAdsTotalSumDStream.groupByKey()

        // TODO 7. 对分组后的数据排序（降序），取前三
        val resultDStream: DStream[(String, Map[String, Long])] = groupDStream.mapValues(datas => {
            datas.toList.sortWith {
                (left, right) => {
                    left._2 > right._2
                }
            }.take(3).toMap
        })

        // TODO 8. 将结果保存到redis中
        resultDStream.foreachRDD(rdd => {
            rdd.foreachPartition(datas => {
                val client: Jedis = RedisUtil.getJedisClient

                datas.foreach {
                    case (key, map) =>

                        val keys: Array[String] = key.split("_")

                        val k: String = "top3_ads_per_day:" + keys(0)
                        val f = keys(1)

                        import org.json4s.JsonDSL._
                        val v: String = JsonMethods.compact(JsonMethods.render(map))

                        client.hset(k, f, v)
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
