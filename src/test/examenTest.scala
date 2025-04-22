package job.examen

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.rdd.RDD

class examenTest extends AnyFlatSpec with Matchers {

  // Usa la sesión de Spark definida en TestInit
  implicit val spark = TestInit.initialize()

  // **Test para Ejercicio 1**
  "Ejercicio 1" should "filtrar estudiantes mayores de 20 años y ordenarlos por edad" in {
    val estudiantes = Seq(
      (1, "Kevin", "Márquez", 25),
      (2, "Ana", "García", 22),
      (3, "Luis", "Fernández", 30),
      (4, "Sofía", "López", 19),
      (5, "Marta", "Pérez", 23)
    ).toDF("id", "nombre", "apellido", "edad")

    val resultado = Examen.ejercicio1(estudiantes).collect()
    resultado.map(row => (row.getString(0), row.getInt(1))) shouldBe List(
      ("Luis", 30),
      ("Kevin", 25),
      ("Marta", 23),
      ("Ana", 22)
    )
  }

  // **Test para Ejercicio 2**
  "Ejercicio 2" should "añadir la columna paridad correctamente" in {
    val numeros = Seq(1, 2, 3, 4, 5).toDF("numero")

    val resultado = Examen.ejercicio2(numeros).collect()
    resultado.map(row => (row.getInt(0), row.getString(1))) shouldBe List(
      (1, "impar"),
      (2, "par"),
      (3, "impar"),
      (4, "par"),
      (5, "impar")
    )
  }

  // **Test para Ejercicio 3**
  "Ejercicio 3" should "calcular los promedios de calificaciones correctamente" in {
    val estudiantes = Seq(
      (1, "Kevin", "Márquez", 25),
      (2, "Ana", "García", 22),
      (3, "Luis", "Fernández", 30)
    ).toDF("id", "nombre", "apellido", "edad")

    val calificaciones = Seq(
      (1, "Matemáticas", 8.5),
      (1, "Física", 9.0),
      (2, "Química", 5.5),
      (2, "Biología", 7.0),
      (3, "Literatura", 6.5),
      (3, "Historia", 7.5)
    ).toDF("id", "materia", "calificacion")

    val resultado = Examen.ejercicio3(estudiantes, calificaciones).collect()
    resultado.map(row => (row.getString(0), row.getDouble(1))) shouldBe List(
      ("Kevin", 8.75),
      ("Ana", 6.25),
      ("Luis", 7.0)
    )
  }

  // **Test para Ejercicio 4**
  "Ejercicio 4" should "contar palabras correctamente" in {
    val palabras = List("error", "warning", "error", "debug", "error", "info", "warning")

    val resultado = Examen.ejercicio4(palabras).collect().toMap
    resultado shouldBe Map(
      "error" -> 3,
      "warning" -> 2,
      "debug" -> 1,
      "info" -> 1
    )
  }

  // **Test para Ejercicio 5**
  "Ejercicio 5" should "calcular el ingreso total por producto" in {
    val ventas = Seq(
      (101, 2, 230.0),
      (102, 3, 135.0),
      (103, 4, 70.0)
    ).toDF("id_producto", "cantidad", "precio_unitario")

    val resultado = Examen.ejercicio5(ventas).collect()
    resultado.map(row => (row.getInt(0), row.getDouble(1))) shouldBe List(
      (101, 460.0),
      (102, 405.0),
      (103, 280.0)
    )
  }
}