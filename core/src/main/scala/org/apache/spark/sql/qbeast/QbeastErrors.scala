package org.apache.spark.sql.qbeast

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

trait QbeastErrorsBase extends QueryErrorsBase {

  def formatSchema(schema: StructType): String = schema.treeString

}

object QbeastErrors extends QbeastErrorsBase

class MetadataMismatchErrorBuilder {
  private var bits: Seq[String] = Nil

  def addSchemaMismatch(original: StructType, data: StructType, id: String): Unit = {
    bits ++=
      s"""A schema mismatch detected when writing to the Qbeast table (Table ID: $id).
         |To enable schema migration using DataFrameWriter or DataStreamWriter, please set:
         |'.option("mergeSchema", "true")'.
         |
         |Table schema:
         |${QbeastErrors.formatSchema(original)}
         |
         |Data schema:
         |${QbeastErrors.formatSchema(data)}
         """.stripMargin :: Nil
  }

  def finalizeAndThrow(conf: SQLConf): Unit = {
    throw new AnalysisException(bits.mkString("\n"))
  }

}
