package org.apache.spark.ml.made


import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.{DenseVector, Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.{BooleanParam, Param, ParamMap}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol, HasLabelCol, HasFitIntercept, HasMaxIter, HasTol}
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row}

import scala.util.control.Breaks.{break,breakable}

trait MyLinearRegressionParams extends HasInputCol with HasOutputCol with HasLabelCol
  with HasFitIntercept with HasTol with HasMaxIter {
  def setInputCol(value: String) : this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)
  def setLabelCol(value: String): this.type = set(labelCol, value)
  setDefault(tol -> 0.000001)
  setDefault(maxIter -> 1000)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, getInputCol, new VectorUDT())

    if (schema.fieldNames.contains($(outputCol))) {
      SchemaUtils.checkColumnType(schema, getOutputCol, new VectorUDT())
      schema
    } else {
      SchemaUtils.appendColumn(schema, schema(getInputCol).copy(name = getOutputCol))
    }
  }
}

class MyLinearRegression(override val uid: String) extends Estimator[MyLinearRegressionModel] with MyLinearRegressionParams
  with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("MyLinearRegression"))

  override def fit(dataset: Dataset[_]): MyLinearRegressionModel = {

    // Used to convert untyped dataframes to datasets with vectors
    implicit val encoder : Encoder[Vector] = ExpressionEncoder()

    val vectors: Dataset[Row] = dataset.select(dataset($(inputCol)), dataset($(labelCol)))

    val dim: Int = AttributeGroup.fromStructField((dataset.schema($(inputCol)))).numAttributes.getOrElse(
      vectors.first().getAs[Vector](0).size
    )
    var weights = Vectors.dense(Array.fill(dim) {
      scala.util.Random.nextDouble
    })
    var bias = scala.util.Random.nextDouble

    def lambda_sum(summarizer: MultivariateOnlineSummarizer, vector: Row): MultivariateOnlineSummarizer = {
      val x = vector.getAs[Vector](0)
      val y = vector.getDouble(1)
      var error = y - x.dot(weights)
      if ($(fitIntercept)) {
        error = error - bias
      }
      var grad = x.asBreeze * (error)
      if ($(fitIntercept)) {
        grad = breeze.linalg.DenseVector.vertcat(grad.toDenseVector, breeze.linalg.DenseVector(error))
      }
      return summarizer.add(mllib.linalg.Vectors.fromBreeze(grad))
    }

    breakable {
      for (n <- 0 to $(maxIter)) {
        val summary1 = vectors.rdd.mapPartitions((data: Iterator[Row]) => {
          val result = data.foldLeft(new MultivariateOnlineSummarizer())(
            lambda_sum
          )
          Iterator(result)
        }).reduce(_ merge _)

        //      print(weights)
        weights = Vectors.fromBreeze(weights.asBreeze + summary1.mean.asBreeze(0 to dim))
        if ($(fitIntercept)) bias = bias + summary1.mean.asBreeze(dim)
        if (summary1.mean.asBreeze.reduce(_ + _).abs < $(tol)) {
          //        println(weights, bias)
          println("Stopped by tolerance on iteration", n)
          break
        }
      }
    }
//    println(weights, bias)
    copyValues(new MyLinearRegressionModel(
      weights, bias)).setParent(this)
  }

  override def copy(extra: ParamMap): Estimator[MyLinearRegressionModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

}

object MyLinearRegression extends DefaultParamsReadable[MyLinearRegression]

class MyLinearRegressionModel private[made](
                                         override val uid: String,
                                         val weights: DenseVector,
                                         val bias: Double
                                       ) extends Model[MyLinearRegressionModel] with MyLinearRegressionParams with MLWritable {


  private[made] def this(weights: Vector, bias: Double) =
    this(Identifiable.randomUID("myLinearRegressionModel"), weights.toDense, bias)

  override def copy(extra: ParamMap): MyLinearRegressionModel = copyValues(
    new MyLinearRegressionModel(weights, bias), extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val w = weights.asBreeze
    val transformUdf = {
      if ($(fitIntercept)) {
        dataset.sqlContext.udf.register(uid + "_transform",
          (x: Vector) => {
            ((x.asBreeze).dot(w)) + bias
          })
      } else {
        dataset.sqlContext.udf.register(uid + "_transform",
          (x: Vector) => {
            ((x.asBreeze).dot(w))
          })
      }
    }

    dataset.withColumn($(outputCol), transformUdf(dataset($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  override def write: MLWriter = new DefaultParamsWriter(this) {
    override protected def saveImpl(path: String): Unit = {
      super.saveImpl(path)

      val vectors = weights.asInstanceOf[Vector]

    }
  }
}
