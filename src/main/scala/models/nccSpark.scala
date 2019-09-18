package models

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.Util

class nccSpark extends Serializable{

  def trainModel(file: String, sc: SparkContext, minPartitions: Int = -1) =
  {
    val data: RDD[(List[Double], String)] = Util.readDataset(file, sc, minPartitions)

    val cardinalities: Map[String, Int] = data.groupBy(_._2).map{case (a, b) => (a, b.size)}.collect().toMap

    data.map{case (a, b) => (b, a)}.reduceByKey(Util.sumListVector)
      .map{case (a, b) => (a, Util.scalarPerVector(1.toDouble/cardinalities(a).toDouble, b))}.collect().toMap

  }

  def classifyPoint(point: List[Double], model: Map[String, List[Double]]): String =
  {
    model.map{case (a, b) => (a, Util.euclideanDistance(point, b))}.toList.minBy(_._2)._1
  }

}
