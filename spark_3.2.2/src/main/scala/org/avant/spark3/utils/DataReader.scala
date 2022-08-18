package org.avant.spark3.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StructType}

import scala.io.Source

trait DataReader extends Spark3Initializer {
  def readAsDF(): DataFrame

  def readAsDFWithSchema(): DataFrame

  def readSchemaFromClassPath(schemaFileName: String) : StructType = {
    val schemaUrl = ClassLoader.getSystemResource(schemaFileName)
    val schemaSource = Source.fromFile(schemaUrl.getFile).getLines().mkString
    DataType.fromJson(schemaSource).asInstanceOf[StructType]
  }
}
