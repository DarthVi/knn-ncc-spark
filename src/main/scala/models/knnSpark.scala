package models

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import utils.Util

class knnSpark extends Serializable {

  def trainModel(file: String, sc: SparkContext, minPartitions: Int = -1): RDD[(List[Double], String)] =
  {
    //read the data and normalize all the vectors
    val data = Util.readDataset(file, sc, minPartitions).map{case (l, str) => (Util.normalize(l), str)}

    //data.persist(StorageLevel.MEMORY_AND_DISK)
    data.cache()
  }

  def classifyPoint(p: List[Double], data: RDD[(List[Double], String)], k: Int): String =
  {
    val point = Util.normalize(p)
    //calculate all the distances of the point from the other points and sort the results in ascending order
    val sortedDistances = data.map{case (a, b) => (b, Util.euclideanDistance(point, a))}.sortBy(_._2, ascending = true)
    //take the top k results
    val topk = sortedDistances.zipWithIndex().filter(_._2 < k)
    //take the most predominant class within the top k
    val result = topk.map(_._1).map(entry => (entry._1, 1)).reduceByKey(_+_).sortBy(_._2, ascending = false).first()._1

    //for debugging purposes
    println(s"Point classified as ${result}")

    result
  }

}
