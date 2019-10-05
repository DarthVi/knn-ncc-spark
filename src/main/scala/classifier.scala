import config.ConfigReader
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import models.{knnSpark, nccSpark}
import utils.Util

object classifier {

  val configReader = new ConfigReader()

  val fileName: String = configReader.getDatasetPath()
  val testPath: String = configReader.getTestPath()
  val k: Int = configReader.getK()

  def main(args: Array[String]): Unit =
  {

    val clusterMode = configReader.getClusterMode()
    val minPartitions = configReader.getMinPartitions()

    var spark: SparkSession = null


    var conf: SparkConf = null

    if (!clusterMode) {

      conf = new SparkConf().setAppName("knnSpark").setMaster("local[*]").set("spark.local.dir", "/home/vincenzo/sparktmp/")
          .set("spark.hadoop.validateOutputSpecs", "false")
    }
    else {
      conf = new SparkConf().setAppName("knnSpark").set("spark.hadoop.validateOutputSpecs", "false")
    }

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val knnSpark = new knnSpark

    var executionTime = 0.0
    var t0 = System.currentTimeMillis()

    val modelKnn = knnSpark.trainModel(fileName, sc, minPartitions)

    val (testClasses, testVector) = Util.readTestset(testPath, sc, minPartitions)

    //cannot call rdd transformation and actions inside other rdd transformation and action due to SPARK-5063 error
    //that's why we use a vector here
    val testVectorV = testVector.collect().toVector
    val classificationKnn: Array[String] = new Array[String](testVectorV.length)

    for(i <- testVectorV.indices.par)
    {
      classificationKnn(i) = knnSpark.classifyPoint(testVectorV(i), modelKnn, k)
    }

    //val classificationKnn = knnSpark.classifyPoint(point, modelKnn, k)
    //val classificationKnn = testVectorV.map(knnSpark.classifyPoint(_, modelKnn, k))

    //save as text file
    val classificationKnnRDD = sc.parallelize(classificationKnn)
    classificationKnnRDD.zipWithIndex.map(_.swap).join(testVector.zipWithIndex.map(_.swap)).values.saveAsTextFile("classificationKnn.txt")


    val knnAccuracy = Util.calculateAccuracy(testClasses, classificationKnnRDD)


    println(s"Classification accuracy with kNN: ${knnAccuracy}")
    executionTime = System.currentTimeMillis() - t0
    println(f"Elapsed time: ${executionTime / 1000d}%1.2f seconds. Method: kNN.")

    executionTime = 0.0
    t0 = System.currentTimeMillis()

    val nccSpark = new nccSpark

    val modelNcc = nccSpark.trainModel(fileName, sc, minPartitions)

    //val classificationNcc = nccSpark.classifyPoint(point, modelNcc)
    val classificationNcc = testVector.map(nccSpark.classifyPoint(_, modelNcc))

    //save as text file
    (classificationNcc zip testVector).saveAsTextFile("classificationNcc.txt")

    val nccAccuracy = Util.calculateAccuracy(testClasses, classificationNcc)

    println(s"Classification accuracy with NCC: ${nccAccuracy}")

    executionTime = System.currentTimeMillis() - t0
    println(f"Elapsed time: ${executionTime / 1000d}%1.2f seconds. Method: NCC.")

  }
}