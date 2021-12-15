package ru.made

import breeze.linalg._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression,LinearRegressionModel}

import org.apache.spark.ml.made.{MyLinearRegression,MyLinearRegressionModel}

import org.apache.log4j.Level
import org.apache.log4j.{Level, Logger}

object Main {
  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      // адрес мастера
      .master("local[*]")
      // имя приложения в интерфейсе спарка
      .appName("made-demo")
      // взять текущий или создать новый
      .getOrCreate()
    import spark.implicits._

    val X = DenseMatrix.rand(100000, 3)
//    val y = X * DenseVector(0.5, -1, 0.2)
    val y = X * DenseVector(1.5, 0.3, -0.7)
    val data = DenseMatrix.horzcat(X, y.asDenseMatrix.t)

    val df = data(*, ::).iterator
      .map(x => (x(0), x(1), x(2), x(3)))
      .toSeq.toDF("x1", "x2", "x3", "y")

    val pipeline1 = new Pipeline().setStages(Array(
      new VectorAssembler()
        .setInputCols(Array("x1", "x2", "x3"))
        .setOutputCol("features"),
      new MyLinearRegression()
        .setInputCol("features").setLabelCol("y").setOutputCol("preds")
    ))
    val model1 = pipeline1.fit(df)
    val w1 = model1.stages.last.asInstanceOf[MyLinearRegressionModel].weights
    model1.transform(df).show(5)
    println(w1)
  }
}
