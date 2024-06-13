package agh.scala.footballanalyzer
package objects.utils

import org.apache.spark.sql.DataFrame

object DataFrameParser {
    def DFtoJsonString(df: DataFrame): String =
        df.toJSON.collect().toList.mkString("[", ",", "]")
}