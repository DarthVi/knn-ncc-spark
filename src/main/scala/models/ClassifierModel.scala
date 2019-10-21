package models

import org.apache.spark.SparkContext

trait ClassifierModel {

  def trainModel(file: String, sc: SparkContext, minPartitions: Int = -1): Unit
  def classifyPoint(p: List[Double]): String
}
