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

import io.qbeast.core.model._
import io.qbeast.spark.index.QbeastColumns
import io.qbeast.spark.writer.RollupDataWriter
import io.qbeast.spark.writer.StatsTracker
import io.qbeast.spark.writer.TaskStats
import io.qbeast.IISeq
import org.apache.spark.sql.execution.datasources.BasicWriteTaskStats
import org.apache.spark.sql.execution.datasources.WriteJobStatsTracker
import org.apache.spark.sql.execution.datasources.WriteTaskStats
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame

import java.util.UUID
import scala.collection.mutable

/**
 * Delta implementation of DataWriter that applies rollup to compact the files.
 */
object HudiRollupDataWriter extends RollupDataWriter {

  override def write(
      tableId: QTableID,
      schema: StructType,
      data: DataFrame,
      tableChanges: TableChanges,
      commitTime: String): IISeq[IndexFile] = {

    if (data.isEmpty) return Seq.empty[IndexFile].toIndexedSeq

    val statsTrackers = StatsTracker.getStatsTrackers

    val extendedData = extendDataWithCubeToRollup(data, tableChanges)
    val hudiData = extendDataWithHudiColumns(extendedData, commitTime)

    val hudiSchema = schema
      .add(StructField("_hoodie_commit_time", StringType, nullable = false))
      .add(StructField("_hoodie_commit_seqno", StringType, nullable = false))
      .add(StructField("_hoodie_record_key", StringType, nullable = false))
      .add(StructField("_hoodie_partition_path", StringType, nullable = false))
      .add(StructField("_hoodie_file_name", StringType, nullable = false))

    val filesAndStats =
      internalWrite(tableId, hudiSchema, hudiData, tableChanges, statsTrackers)
    val stats = filesAndStats.map(_._2)

    processStats(stats, statsTrackers)

    filesAndStats
      .map(_._1)
  }

  private def processStats(
      stats: IISeq[TaskStats],
      statsTrackers: Seq[WriteJobStatsTracker]): Unit = {
    val basicStatsBuilder = Seq.newBuilder[WriteTaskStats]
    var endTime = 0L
    stats.foreach(stats => {
      basicStatsBuilder ++= stats.writeTaskStats.filter(_.isInstanceOf[BasicWriteTaskStats])
      endTime = math.max(endTime, stats.endTime)
    })
    val basicStats = basicStatsBuilder.result()
    statsTrackers.foreach(_.processStats(basicStats, endTime))
  }

  private def extendDataWithHudiColumns(
      extendedData: DataFrame,
      commitTime: String): DataFrame = {

    val generateToken = udf((rank: Int) => {
      s"$rank-${rank + 13}-0"
    })

    def generateRecordKey(timestamp: String): UserDefinedFunction =
      udf((rank: Int, id: Int) => {
        s"${timestamp}_${rank}_$id"
      })

    def generateCommitSeqno(timestamp: String): UserDefinedFunction =
      udf((rank: Int, id: Int) => {
        s"${timestamp}_${rank}_$id"
      })

    val windowSpec = Window.partitionBy("_qbeastCubeToRollup").orderBy("_qbeastCubeToRollup")
    val windowSpecGroup = Window.partitionBy("_qbeastCubeToRollup").orderBy("_qbeastCubeToRollup")

    val df = extendedData
      .withColumn("_token", generateToken(dense_rank().over(windowSpec) - 1))
      .withColumn("_hoodie_commit_time", lit(commitTime))
      .withColumn(
        "_hoodie_commit_seqno",
        generateCommitSeqno(commitTime)(
          dense_rank().over(windowSpec) - 1,
          row_number().over(windowSpec) - 1))
      .withColumn(
        "_hoodie_record_key",
        generateRecordKey(commitTime)(
          dense_rank().over(windowSpec) - 1,
          row_number().over(windowSpecGroup) - 1))
      .withColumn("_hoodie_partition_path", lit(""))
      .withColumn(
        "_hoodie_file_name",
        concat(
          col(QbeastColumns.cubeToRollupColumnName),
          lit("-0_"),
          col("_token"),
          lit("_"),
          lit(commitTime),
          lit(".parquet")))
      .withColumn(QbeastColumns.cubeToRollupFileName, col("_hoodie_file_name"))
      .drop("_token")

    // df.show(10, false)

    df
  }

}
