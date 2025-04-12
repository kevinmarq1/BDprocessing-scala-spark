package job.examen

import org.scalatest.funsuite.AnyFunSuiteLike

class examenTest extends AnyFunSuiteLike {
  import SparkSessionProvider.spark

  object TestEjercicio3 {
    def main(args: Array[String]): Unit = {
      val resultados = examen.ejercicio3()
      resultados.show()
    }
  }

}
