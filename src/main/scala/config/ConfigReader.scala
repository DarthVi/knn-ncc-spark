package config

import java.io.FileInputStream
import java.util.Properties

class ConfigReader {

  val propResource = new FileInputStream("classifier.properties")

  val properties = new Properties()

  properties.load(propResource)

  def getDatasetPath(): String =
  {
    properties.getProperty("classifier.filepath")
  }

  def getTestPath(): String =
  {
    properties.getProperty("classifier.testpath")
  }

  def getK(): Int =
  {
    Integer.parseInt(properties.getProperty("knn.k"))
  }

  def getClusterMode(): Boolean =
  {
    properties.getProperty("classifier.clusterMode").toBoolean
  }

  def getMinPartitions(): Int =
  {
    Integer.parseInt(properties.getProperty("classifier.minPartitions"))
  }

  def getNumPoints(): Int =
  {
    Integer.parseInt(properties.getProperty("genfile.numPoints"))
  }

  def getDistance(): Double =
  {
    properties.getProperty("genfile.distance").toDouble
  }

  def getKnnSaveLocation(): String =
  {
    properties.getProperty("classifier.knnSaveLocation")
  }

  def getNccSaveLocation(): String =
  {
    properties.getProperty("classifier.nccSaveLocation")
  }

  def close(): Unit =
  {
    propResource.close()
  }
}
