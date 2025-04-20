package job.examen

import org.apache.spark.sql.SparkSession

// Este objeto se encarga de inicializar la sesión de Spark para los tests.
object TestInit {

  // Configuración básica para usar Spark en modo local durante las pruebas.
  val spark: SparkSession = SparkSession.builder
    .appName("Test de Practica")
    .master("local[*]") // Utiliza todos los núcleos disponibles en tu máquina.
    .config("spark.driver.memory", "512m") // Asigna memoria para evitar problemas de ejecución.
    .config("spark.ui.enabled", "false") // Evita mostrar la interfaz de Spark para pruebas simples.
    .getOrCreate()

  // Método simple para devolver la instancia de Spark. Esto asegura flexibilidad en otros archivos.
  def initialize(): SparkSession = spark
}
