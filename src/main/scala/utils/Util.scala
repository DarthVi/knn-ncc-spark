package utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object Util {

  def euclideanDistance(l1: List[Double], l2: List[Double]): Double =
  {
    val zipped = l1 zip l2

    math.sqrt(zipped.map{case (a, b) => math.pow(a - b, 2)}.sum)
  }

  def readDataset(file: String, sc: SparkContext, minPartitions: Int = -1): RDD[(List[Double], String)] =
  {
    val input = sc.textFile(file)

    val classes: RDD[String] = input.map(_.split("\t").takeRight(1).mkString(""))
    val vector = input.map(_.split("\t").dropRight(1).map(_.toDouble).toList)
//    val classes: RDD[String] = input.map(_.split("\t").take(1).mkString(""))
//    val vector = input.map(_.split("\t").drop(1).map(_.toDouble).toList)

    val data = vector zip classes

    data
  }

  def sumListVector(l1: List[Double], l2: List[Double]): List[Double] =
  {
    (l1 zip l2).map{case (a, b) => a + b}
  }

  def scalarPerVector(scalar: Double, l: List[Double]): List[Double] =
  {
    l.map(_ * scalar)
  }
}
