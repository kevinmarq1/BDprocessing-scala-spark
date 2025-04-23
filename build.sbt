ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "Practica",
    libraryDependencies ++= Seq(
      // Dependencias necesarias para Spark
      "org.apache.spark" %% "spark-core" % "3.3.0",
      "org.apache.spark" %% "spark-sql" % "3.3.0",
      // ScalaTest para pruebas
      "org.scalatest" %% "scalatest" % "3.2.15" % Test
    ),
    // Opciones para la JVM para optimizar memoria y evitar errores de Metaspace
    javaOptions ++= Seq(
      "-Xms512m",               // Memoria mínima asignada
      "-Xmx1g",                 // Memoria máxima asignada
      "-XX:MetaspaceSize=256m", // Tamaño inicial de Metaspace
      "-XX:MaxMetaspaceSize=512m", // Tamaño máximo de Metaspace
      "-XX:+UseG1GC"            // Activar el recolector de basura G1
    ),
    // Estrategia de carga de clases para evitar problemas de Metaspace en pruebas
    Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.ScalaLibrary
  )
