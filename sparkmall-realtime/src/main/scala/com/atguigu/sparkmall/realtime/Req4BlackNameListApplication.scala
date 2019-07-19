package com.atguigu.sparkmall.realtime


import com.atguigu.sparkmall.common.util.DateUtil
import com.atguigu.sparkmall.realtime.util.{KafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object Req4BlackNameListApplication {

    def main(args: Array[String]): Unit = {

        // 需求四：广告黑名单实时统计

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

        /*
        adsClickDStream.foreachRDD{
            rdd => {
                rdd.foreach(println)
            }
        }
        */

        // TODO 0. 对数据进行筛选过滤，黑名单数据不需要
        // Driver


        /*
        // 问题1 ： 会发生空指针异常，是因为序列化规则
        val filterDStream: DStream[AdsClickKafkaMessage] = adsClickDStream.filter(message => {
            // Executor
            !useridsBroadcast.value.contains(message.userid)
        })
        */

        // 问题2 ：黑名单数据无法更新，应该周期性的获取最新黑名单数据
        // Driver(1)
        val filterDStream: DStream[AdsClickKafkaMessage] = adsClickDStream.transform(rdd => {
            // Drvier(N)
            val jedisClient: Jedis = RedisUtil.getJedisClient
            val userids: java.util.Set[String] = jedisClient.smembers("blacklist")
            jedisClient.close()
            // 使用广播变量
            val useridsBroadcast: Broadcast[java.util.Set[String]] = streamingContext.sparkContext.broadcast(userids)
            rdd.filter(message => {
                // Executor(M)
                !useridsBroadcast.value.contains(message.userid)
            })
        })

        // TODO 1. 将数据转换结构 （date-ads-user, 1）
        val dateAdsUserToOneDStream: DStream[(String, Long)] = filterDStream.map(message => {
            val date: String = DateUtil.formatStringByTimestamp(message.timestamp.toLong, "yyyy-MM-dd")
            (date + "_" + message.adid + "_" + message.userid, 1L)
        })

        // TODO 2. 将转换结构后的数据进行有状态聚合 （date-ads-user, sum）
        val stateDStream: DStream[(String, Long)] = dateAdsUserToOneDStream.updateStateByKey[Long] {
            (seq: Seq[Long], buffer: Option[Long]) => {
                val sum: Long = buffer.getOrElse(0L) + seq.size
                Option(sum)
            }
        }

        // TODO 3. 对聚合后的结果进行阈值的判断
        // redis 五大数据类型 set
        // jedis
        stateDStream.foreachRDD(rdd=>{
            rdd.foreach{
                case ( key, sum ) => {
                    if ( sum >= 100 ) {
                        // TODO 4. 如果超出阈值，将用户拉入黑名单
                        val keys: Array[String] = key.split("_")
                        val userid = keys(2)

                        val client: Jedis = RedisUtil.getJedisClient
                        client.sadd("blacklist", userid)
                        client.close()
                    }
                }
            }
        })

        // 启动采集器
        streamingContext.start()
        // Driver应该等待采集器的执行结束
        streamingContext.awaitTermination()
    }
}
case class AdsClickKafkaMessage(timestamp : String, area:String, city:String, userid:String, adid:String)