package job.examen

import org.apache.spark.sql.SparkSession

object SparkSessionProvider {
  implicit val spark: SparkSession = SparkSession.builder
    .appName("Practica")
    .master("local[*]")
    .getOrCreate()
}
