package com.atguigu.spark.offline

import com.atguigu.sparkmall.common.model.UserVisitAction
import com.atguigu.sparkmall.common.util.{ConfigUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Req3PageFlowApplication {

    def main(args: Array[String]): Unit = {

        // 需求三： 页面单跳转转化率统计

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

        // TODO 4.1 计算分母的数据
        // TODO　4.1.1 将原始数据进行旧结构的转换，用于统计分析
        val pageids: Array[String] = ConfigUtil.getValueByJsonKey("targetPageFlow").split(",")

        val pageflowIds: Array[String] = pageids.zip(pageids.tail).map {
            case (pageid1, pageid2) =>
                pageid1 + "-" + pageid2
        }

        val filterRDD: RDD[UserVisitAction] = actionRDD.filter(action => {
            pageids.contains(action.page_id.toString)
        })

        val pageMapRDD: RDD[(Long, Long)] = filterRDD.map(action => {
            (action.page_id, 1L)
        })

        // TODO 4.1.2 将转换后的结果进行聚合统计 ( pageid, 1 )
        val pageIdToSumRDD: RDD[(Long, Long)] = pageMapRDD.reduceByKey(_+_)

        val pageIdToSumMap: Map[Long, Long] = pageIdToSumRDD.collect().toMap

        // TODO 4.2 计算分子的数据
        // TODO 4.2.1 将原始数据通过session进行分组 ( sessionId, iterator[( pageid, action_time )] )
        val groupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(action=>action.session_id)

        // TODO 4.2.2 将分组后的数据使用时间进行排序 --升序
        val sessionToPageflowRDD: RDD[(String, List[(String, Long)])] = groupRDD.mapValues(data => {
            val actions: List[UserVisitAction] = data.toList.sortWith {
                (left, right) => {
                    left.action_time < right.action_time
                }
            }

            val sessionPageids: List[Long] = actions.map(_.page_id)
            // (1-2,1), (2-3,1), 3-4, 4-5, 5-6, 6-7
            // TODO 4.2.3 将排序后的数据进行拉链处理，形成单跳页面流转顺序
            val pageid1ToPageid2s: List[(Long, Long)] = sessionPageids.zip(sessionPageids.tail)
            pageid1ToPageid2s.map {
                case (pageid1, pageid2) =>
                    // TODO 4.2.4 将处理后的数据进行筛选过滤，保留需要关心的流转数据（session, pageid1-pageid2, 1）
                    (pageid1 + "-" + pageid2, 1L)
            }
        })

        // TODO 4.2.5 对过滤后的数据进行结构的转化（pageid1-pageid2, 1）
        val mapRDD: RDD[List[(String, Long)]] = sessionToPageflowRDD.map(_._2)

        val flatMapRDD: RDD[(String, Long)] = mapRDD.flatMap(list=>list)

        val finalPageflowRDD: RDD[(String, Long)] = flatMapRDD.filter {
            case (pageflow, one) =>
                pageflowIds.contains(pageflow)
        }

        // TODO 4.2.6 将转换结构后的数据进行聚合统计（pageid1-pageid2, sumcount1）
        val resultRDD: RDD[(String, Long)] = finalPageflowRDD.reduceByKey(_+_)

        // TODO 4.3 使用分子数据除以分母数据：（sumcount1 / sumcount）
        resultRDD.foreach{
            case(pageflow, sum) =>
                val ids: Array[String] = pageflow.split("-")
                val pageid = ids(0)
                val sum1: Long = pageIdToSumMap.getOrElse(pageid.toLong, 1L)

                println(pageflow + " = " + ( sum.toDouble / sum1 ))
        }

        // TODO 4.7 释放资源
        spark.stop()

    }

}
