package job.examen

import org.apache.spark.sql.SparkSession

object TestInit {
  def initialize(): SparkSession = SparkSessionProvider.spark
}
