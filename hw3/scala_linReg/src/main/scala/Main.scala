
import breeze.linalg._
import breeze.io.CSVReader

object Main {
    def main(args: Array[String]): Unit = {
        val file = new java.io.File(args(0))
        val mat = csvread(file)
        val m = mat.cols
        val n = mat.rows

        var X = mat(::, 1 to m-1)
        val y = mat(::, 0)

        println(args.length)
        if ((args.length > 1) & (args(1) == "add_const")) {
            val const_vec = DenseMatrix.ones[Double](n, 1)
            X = DenseMatrix.horzcat(const_vec, X)
        }


        println(inv(X.t * X) * X.t * y)
    }
}
