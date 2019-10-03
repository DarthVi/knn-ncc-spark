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

  def norm(l: List[Double]) : List[Double] =
  {
    val norm: Double = Math.sqrt(l.map(x => x*x).sum)

    val normalized = scalarPerVector(1.0/norm, l)

    normalized
  }

  def normalize(data: RDD[(List[Double], String)]) : RDD[(List[Double], String)] =
  {
    val vectors = data.map(_._1.toVector)
    val classes = data.map(_._2)
    val vLength = vectors.take(1).head.length

    val minS = new Array[Double](vLength)
    val maxS = new Array[Double](vLength)
    val meanS = new Array[Double](vLength)

    for(i <- 0 until vLength)
    {
      minS(i) = vectors.map(x => x(i)).min()
      maxS(i) = vectors.map(x => x(i)).max()
      meanS(i) = vectors.map(x => x(i)).mean()
    }

    val normalizedVectors = vectors.map{x => {
      val normS = new Array[Double](x.length)
      for(i <- x.indices)
        {
          normS(i) = (x(i) - meanS(i))/(maxS(i) - minS(i))
        }
      normS.toVector
    }}

    val normalizedList = normalizedVectors.map(_.toList)

    normalizedList zip classes



  }
}
