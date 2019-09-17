import java.io._

import config.ConfigReader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import models.{knnSpark, nccSpark}
import org.apache.spark.storage.StorageLevel
import utils.Util

import scala.util.Random

object classifier {

  val configReader = new ConfigReader()

  val fileName: String = configReader.getDatasetPath()
  val k: Int = configReader.getK()

  val randomX = new Random
  val randomY = new Random

  //generazione dati sintetici su cui testare l'algoritmo
  def genFile() = {

    //4 punti di aggregazione iniziali
    val initPoints = Vector((50.0,50.0, "A"),(50.0,-50.0, "B"),(-50.0,50.0, "C"),(-50.0,-50.0, "D"))
    val configReader = new ConfigReader()

    val distance = configReader.getDistance()
    val numPoints = configReader.getNumPoints()

    val randomPoint = new Random

    //generare un numero di punti attorno alle 4 zone con una massima distanza

    val file = new File(fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    for (i <- 0 until numPoints) {
      val x = (randomX.nextDouble-0.5) * distance
      val y = (randomX.nextDouble-0.5) * distance
      val centroid = initPoints(randomPoint.nextInt(initPoints.length))
      bw.write((centroid._1+x)+"\t"+(centroid._2 + y)+"\t"+centroid._3+"\n")
    }
    bw.close
  }

  def main(args: Array[String]): Unit =
  {

    val point = args.map(_.toDouble).toList
    val configReader = new ConfigReader()

    genFile()

    val clusterMode = configReader.getClusterMode()
    val minPartitions = configReader.getMinPartitions()

    var spark: SparkSession = null

    if(!clusterMode)
    {
      spark = SparkSession.builder()
        .appName("AprioriSpark")
        .master("local[*]")
        .getOrCreate()
    }
    else
    {
      spark = SparkSession.builder().appName("AprioriSpark").getOrCreate()
    }

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val knnSpark = new knnSpark

    val modelKnn = knnSpark.trainModel(fileName, sc, minPartitions)

    val classificationKnn = knnSpark.classifyPoint(point, modelKnn, k)

    println(s"Classification according to kNN: ${classificationKnn}")

    val nccSpark = new nccSpark

    val modelNcc = nccSpark.trainModel(fileName, sc, minPartitions)

    val classificationNcc = nccSpark.classifyPoint(point, modelNcc)

    println(s"Classification according to NCC: ${classificationNcc}")

  }
}