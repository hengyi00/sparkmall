package com.atguigu.spark.offline

import com.atguigu.sparkmall.common.model.UserVisitAction
import com.atguigu.sparkmall.common.util.{ConfigUtil, DateUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Req8PageAvgTimeApplication {

    def main(args: Array[String]): Unit = {

        // 需求八： 页面的平均停留时间

        // TODO 4.0 创建Spark SQL的环境对象
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req3PageFlowApplication")

        val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
        spark.sparkContext.setCheckpointDir("cp")

        import spark.implicits._

        // TODO 4.1 从Hive中获取满足条件的数据
        spark.sql("use " + ConfigUtil.getValueByKey("hive.database"))
        var sql = " select * from user_visit_action where 1 = 1 "

        // 获取条件
        val startDate: String = ConfigUtil.getValueByJsonKey("startDate")
        val endDate: String = ConfigUtil.getValueByJsonKey("endDate")

        if (StringUtil.isNotEmpty(startDate)) {
            sql = sql + " and date >= '" + startDate + "' "
        }

        if (StringUtil.isNotEmpty(endDate)) {
            sql = sql + " and date <= '" + endDate + "' "
        }

        val actionDF: DataFrame = spark.sql(sql)
        val actionDS: Dataset[UserVisitAction] = actionDF.as[UserVisitAction]
        val actionRDD: RDD[UserVisitAction] = actionDS.rdd

        actionRDD.checkpoint()

        // TODO 1. 将数据根据session进行分组
        val groupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)

        // TODO 2. 将分组后的数据进行时间排序（升序）
        val sessionToPageidAndTimeRDD: RDD[(String, List[(Long, Long)])] = groupRDD.mapValues(datas => {
            val sortList: List[UserVisitAction] = datas.toList.sortWith {
                (left, right) => {
                    left.action_time < right.action_time
                }
            }

            // TODO 3. 将页面数据进行拉链  ( (1-2), (time2-time1) )
            val idToTimeList: List[(Long, String)] = sortList.map(action => {
                (action.page_id, action.action_time)
            })
            // ( (pageid1, time1), (pageid2, time2) )
            val pageid1ToPageid2List: List[((Long, String), (Long, String))] = idToTimeList.zip(idToTimeList.tail)

            // TODO 4. 将拉链数据进行结构的转变  (1, (timeX)), (1, (timeX)), (2, (timeX))
            pageid1ToPageid2List.map {
                case (page1, page2) =>
                    val time1: Long = DateUtil.parseLongByString(page1._2)
                    val time2: Long = DateUtil.parseLongByString(page2._2)
                    val timeX: Long = time2 - time1

                    (page1._1, timeX)
            }
        })

        // TODO 5. 将转变后的数据进行分组 (pageid, Iterator[(time)])
        val pageidToTimeXListRDD: RDD[List[(Long, Long)]] = sessionToPageidAndTimeRDD.map {
            case (_, v) => v
        }
        val pageidToTimeXRdd: RDD[(Long, Long)] = pageidToTimeXListRDD.flatMap(list=>list)

        val groupPageidRDD: RDD[(Long, Iterable[Long])] = pageidToTimeXRdd.groupByKey()

        // TODO 6. 获取最终结果 (pageid, timeSum / timeSize)

        groupPageidRDD.foreach{
            case (pageid, timex) =>
                println("页面" + pageid + " 平均停留时间 = " + (timex.sum / timex.size))
        }

        // 释放资源
        spark.stop()

    }

}
