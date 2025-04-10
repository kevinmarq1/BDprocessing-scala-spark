class PracticaTest {
  import org.scalatest.flatspec.AnyFlatSpec
  import java.io.{ByteArrayOutputStream, PrintStream}

  class PracticaTest extends AnyFlatSpec {
    "The main method" should "print Hello world!" in {
      // Redirigir la salida estándar
      val outCapture = new ByteArrayOutputStream()
      Console.setOut(new PrintStream(outCapture))

      // Ejecutar el método main
      Main.main(Array.empty)

      // Validar el resultado
      assert(outCapture.toString.trim == "Hello world!")
    }
  }

}
