package models

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.Util

class nccSpark extends Serializable{

  def trainModel(file: String, sc: SparkContext, minPartitions: Int = -1): Map[String, List[Double]] =
  {
    //read the data and normalize the vectors
    val data: RDD[(List[Double], String)] = Util.readDataset(file, sc, minPartitions)
    //for each class, calculate how many points belongs to it and save the result in a map
    //val cardinalities: Map[String, Int] = data.groupBy(_._2).map{case (a, b) => (a, b.size)}.collect().toMap
    val cardinalities: Map[String, Int] = data.map{case (a, b) => (b, 1)}.reduceByKey(_+_).collect().toMap
    //apply the formula to calcolate the centroids: for each class, sum the components of all the points belonging to it
    //and then divide by the cardinality of the class
    data.map{case (a, b) => (b, a)}.reduceByKey(Util.sumListVector)
      .map{case (a, b) => (a, Util.scalarPerVector(1.toDouble/cardinalities(a).toDouble, b))}.collect().toMap

  }

  def classifyPoint(p: List[Double], model: Map[String, List[Double]]): String =
  {
    //for each centroid calculate the distance from the point and take the minimum
    model.map{case (a, b) => (a, Util.euclideanDistance(p, b))}.toList.minBy(_._2)._1
  }

}
