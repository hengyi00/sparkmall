package com.atguigu.spark.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.atguigu.sparkmall.common.model.UserVisitAction
import com.atguigu.sparkmall.common.util.{ConfigUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}

object Req1HotCategoryTop10Application {

    def main(args: Array[String]): Unit = {
        //需求1，获取点击，下单和支付数量排名前10的品类


        //4.0创建SparkSql环境对象
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1HotCaegoryTop10Application")

        val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        import spark.implicits._

        //4.1从hive中获取满足条件的数据
        spark.sql("use " + ConfigUtil.getValueByKey("hive.database"))
        var sql = " select * from user_visit_action where 1 = 1 "

        val startDate: String = ConfigUtil.getValueByJsonKey("startDate")
        val endDate: String = ConfigUtil.getValueByJsonKey("endDate")

        // 增加条件
        if ( StringUtil.isNotEmpty(startDate) ) {
             sql = sql + " and date >= '" + startDate + "' "
        }

        if ( StringUtil.isNotEmpty(endDate) ) {
            sql = sql + " and date <= '" + endDate + "' "
        }

        val actionDF: DataFrame = spark.sql(sql)
        val actionDS: Dataset[UserVisitAction] = actionDF.as[UserVisitAction]
        val actionRDD: RDD[UserVisitAction] = actionDS.rdd


        //4.2使用累加器累加数据，进行数据的聚合(categoryid-click,100), (categoryid-order,100), (categoryid-pay,100)
        val accumulator = new CategoryAccumulator
        spark.sparkContext.register(accumulator, "Category")

        actionRDD.foreach{
            actionData => {
                if ( actionData.click_category_id != -1){
                    accumulator.add(actionData.click_category_id + "-click")
                } else if ( StringUtil.isNotEmpty(actionData.order_category_ids) ) {
                    val ids: Array[String] = actionData.order_category_ids.split(",")
                    for (id <- ids ) {
                        accumulator.add(id + "-order")
                    }
                } else if ( StringUtil.isNotEmpty(actionData.order_category_ids) ) {
                    val ids: Array[String] = actionData.pay_category_ids.split(",")
                    for (id <- ids ) {
                        accumulator.add(id + "-pay")
                    }
                }
            }
        }
        // 输出 => (category-指标, sumcount)
        val accuData: mutable.HashMap[String, Long] = accumulator.value

        //4.3将累加器的结果通过ID进行分组(categoryid,[(order,100),(click:100),(pay:100)])
        val categoryToAccuData: Map[String, mutable.HashMap[String, Long]] = accuData.groupBy {
            case (key, _) =>
                val keys: Array[String] = key.split("-")
                keys(0)
        }

        //4.4将分组后的结果转化为对象CategoryTop10(categoryid,click,pay)
        val taskId: String = UUID.randomUUID().toString

        val categoryTop10Datas: immutable.Iterable[CategoryTop10] = categoryToAccuData.map {
            case (categoryId, map) =>
                CategoryTop10(
                    taskId,
                    categoryId,
                    map.getOrElse(categoryId + "-click", 0L),
                    map.getOrElse(categoryId + "-order", 0L),
                    map.getOrElse(categoryId + "-pay", 0L))
        }

        //4.5将转换后的对象进行排序(点击，下单，支付)（降序）
        val sortList: List[CategoryTop10] = categoryTop10Datas.toList.sortWith {
            (left, right) => {
                if (left.clickCount > right.clickCount) {
                    true
                } else if (left.clickCount == right.clickCount) {
                    if (left.orderCount > right.orderCount) {
                        true
                    } else if (left.orderCount == right.orderCount) {
                        left.payCount > right.payCount
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
        }

        //4.6将排序后的结果取前10保存到Mysql中
        val top10List: List[CategoryTop10] = sortList.take(10)

        val driverClass: String = ConfigUtil.getValueByKey("jdbc.driver.class")
        val url: String = ConfigUtil.getValueByKey("jdbc.url")
        val user: String = ConfigUtil.getValueByKey("jdbc.user")
        val password: String = ConfigUtil.getValueByKey("jdbc.password")

        Class.forName(driverClass)
        val connection: Connection = DriverManager.getConnection(url, user, password)

        val insertSQL = "insert into category_top10 ( taskId, category_id, click_count, order_count, pay_count ) values (?, ?, ?, ?, ?)"

        val pstat: PreparedStatement = connection.prepareStatement(insertSQL)

        top10List.foreach{
            data => {
                pstat.setString(1, data.taskId)
                pstat.setString(2, data.categoryId)
                pstat.setLong(3, data.clickCount)
                pstat.setLong(4, data.orderCount)
                pstat.setLong(5, data.payCount)
                pstat.execute()
            }
        }

        pstat.close()
        connection.close()

        //4.7释放资源
        spark.stop()

    }

}
case class CategoryTop10( taskId:String, categoryId:String, clickCount:Long, orderCount:Long, payCount:Long)
class CategoryAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

    var map = new mutable.HashMap[String, Long]()

    override def isZero: Boolean = {
        map.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
        new CategoryAccumulator
    }

    override def reset(): Unit = {
        map.clear()
    }

    override def add(v: String): Unit = {
        // getOrElse(v,0L) 根据v来取值，取不到为0
        map(v) = map.getOrElse(v, 0L) + 1
    }

    override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {

        //合并map
        val map1: mutable.HashMap[String, Long] = map
        val map2: mutable.HashMap[String, Long] = other.value

        map = map1.foldLeft(map2){
            (innerMap, t) => {
                innerMap(t._1) = innerMap.getOrElse(t._1, 0l) + t._2
                innerMap
            }
        }
    }

    override def value: mutable.HashMap[String, Long] = {
        map
    }
}