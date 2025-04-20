package job.examen

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// Clase para pruebas relacionadas con los datos del proyecto.
class examenTest extends AnyFlatSpec with Matchers {

  // Usa la sesión de Spark definida en TestInit.
  implicit val spark = TestInit.initialize()

  // Comprueba si el archivo CSV se carga correctamente.
  "Lectura del CSV" should "funcionar correctamente" in {
    val datos: DataFrame = spark.read
      .option("header", "true") // Indica que el archivo tiene una fila de encabezado.
      .csv("src/test/resources/ventas.csv") // Ruta del archivo CSV para las pruebas.

    // Verifica que el archivo no esté vacío.
    datos.count() should be > 0L
  }

  // Valida una transformación simple en los datos.
  "Transformaciones en los datos" should "añadir la columna total correctamente" in {
    val datos: DataFrame = spark.read
      .option("header", "true")
      .csv("src/test/resources/ventas.csv")

    // Añade una columna calculada 'total' (cantidad * precio_unitario).
    val transformados = datos.withColumn("total", datos("cantidad") * datos("precio_unitario"))

    // Asegúrate de que la columna 'total' existe.
    transformados.columns should contain("total")
  }
}
