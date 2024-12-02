/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.hudi

import io.qbeast.QbeastIntegrationTestSpec
import org.apache.avro.Schema
import org.apache.hudi.common.model.HoodieAvroPayload
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

class HudiQbeastCatalogTest extends QbeastIntegrationTestSpec {

  def createMetaClient(jsc: JavaSparkContext, basePath: String): HoodieTableMetaClient = {
    HoodieTableMetaClient.builder
      .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
      .setBasePath(basePath)
      .build
  }

  def createTestData(spark: SparkSession, numerOfRows: Int): DataFrame = {
    import spark.implicits._
    StudentGenerator.generateStudents(numerOfRows).toDF()
  }

  def createHoodieRecordsFromDataFrame(
      df: DataFrame,
      schema: Schema): JavaRDD[HoodieRecord[HoodieAvroPayload]] = {
    df.rdd.map(row => HudiUtils.createHoodieRecordFromRow(row, schema)).toJavaRDD()
  }

  val hudiSparkConf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    .set("hoodie.datasource.write.precombine.field", "QBEAST")
    .set("hoodie.metadata.enable", "true")
    .set("hoodie.enable.data.skipping", "true")
    .set("hoodie.metadata.index.column.stats.enable", "true")
    .set("spark.qbeast.tableFormat", "hudi")

  "Hudi Qbeast Catalog" should
    "create a table" in withExtendedSparkAndTmpDir(hudiSparkConf) { (spark, tmpDir) =>
      val sqlQuery1 = """
        create table student_parquet(id int, name string, age int) USING parquet
      """
      spark.sql(sqlQuery1)
      spark.sql("INSERT INTO student_parquet VALUES (1, 'John', 20L)")

      val sqlQuery3 = """
        create table student_hudi(id int, name string, age int) USING hudi OPTIONS ('columnsToIndex'='id')
      """
      spark.sql(sqlQuery3)
      spark.sql("INSERT INTO student_hudi SELECT * FROM student_parquet")

    }

  "Hudi Qbeast Catalog" should
    "evolve schema" in withExtendedSparkAndTmpDir(hudiSparkConf) { (spark, tmpDir) =>
      import spark.implicits._

      removeDirectory("spark-warehouse/student")

      spark.sql(
        "CREATE TABLE student (id INT) USING hudi " +
          "OPTIONS ('columnsToIndex'='id')")
      spark.sql("INSERT INTO student VALUES (1), (2)")

      val format = "qbeast"

      val df = Seq(1, 2).toDF("id")
      val path = s"$tmpDir/student"
      df.write
        .format(format)
        .mode("overwrite")
        .option("hoodie.table.name", "students")
        .option("columnsToIndex", "id")
        .save(path)

      val dfExtraCol = Seq((1, "John"), (2, "Doe")).toDF("id", "name")
      dfExtraCol.write
        .format(format)
        .mode("append")
        // .option("hoodie.write.set.null.for.missing.columns", "true")
        .option("columnsToIndex", "id")
        .save(path)

      spark.read
        .format(format)
        .load(path)
        .show(false)

      val renamedDF = Seq((1, "John", 10)).toDF("id", "name2", "age")

      renamedDF.write
        .format(format)
        .mode("append")
        // .option("hoodie.write.set.null.for.missing.columns", "true")
        .option("columnsToIndex", "id")
        .save(path)

      spark.read
        .format(format)
        .load(path)
        .show(false)
    }

}
