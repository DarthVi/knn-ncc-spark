package models

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import utils.Util

class knnSpark {

  def trainModel(file: String, sc: SparkContext, minPartitions: Int = -1): RDD[(List[Double], String)] =
  {
    val data = Util.readDataset(file, sc, minPartitions)

    data.persist(StorageLevel.DISK_ONLY)
  }

  def classifyPoint(point: List[Double], data: RDD[(List[Double], String)], k: Int): String =
  {
    val sortedDistances = data.map{case (a, b) => (b, Util.euclideanDistance(point, a))}.sortBy(_._2, ascending = true)

    val topk = sortedDistances.zipWithIndex().filter(_._2 < k)

    topk.map(_._1).map(entry => (entry._1, 1)).reduceByKey(_+_).sortBy(_._2, ascending = false).first()._1
  }

}
