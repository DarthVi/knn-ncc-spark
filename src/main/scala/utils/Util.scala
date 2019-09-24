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
    var input: RDD[String] = null

    if(minPartitions != -1)
      input = sc.textFile(file, minPartitions)
    else
      input = sc.textFile(file)


//    val classes: RDD[String] = input.map(_.split("\t").takeRight(1).mkString(""))
//    val vector = input.map(_.split("\t").dropRight(1).map(_.toDouble).toList)
    val classes: RDD[String] = input.map(_.split(",").take(1).mkString(""))
    val vector = input.map(_.split(",").drop(1).map(_.toDouble).toList)

    val data = vector zip classes

    data
  }

  def readTestset(file: String, sc: SparkContext, minPartitions: Int = -1): (RDD[String], RDD[List[Double]]) =
  {
    val data = readDataset(file, sc, minPartitions)

    val classes = data.map(_._2)
    val vector = data.map(_._1)

    (classes, vector)
  }

  def calculateAccuracy(v1: RDD[String], v2: RDD[String]): Double =
  {
    //https://stackoverflow.com/questions/40405891/cant-zip-rdds-with-unequal-number-of-partitions-what-can-i-use-as-an-alternati
    //trick to solve "can't zip RDDs with unequal number of partitions
    v1.zipWithIndex.map(_.swap).join(v2.zipWithIndex.map(_.swap)).values.map{case (a, b) => if (a == b) 1 else 0}.sum()/v1.count()
  }

  def sumListVector(l1: List[Double], l2: List[Double]): List[Double] =
  {
    (l1 zip l2).map{case (a, b) => a + b}
  }

  def scalarPerVector(scalar: Double, l: List[Double]): List[Double] =
  {
    l.map(_ * scalar)
  }

  def normalize(l: List[Double]) : List[Double] =
  {
    val norm: Double = Math.sqrt(l.map(x => x*x).sum)

    val normalized = scalarPerVector(1.0/norm, l)

    normalized
  }
}
