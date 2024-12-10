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
import io.qbeast.spark.writer.RollupDataWriter
import io.qbeast.spark.writer.StatsTracker
import io.qbeast.spark.writer.TaskStats
import io.qbeast.IISeq
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieRecord
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.BasicWriteTaskStats
import org.apache.spark.sql.execution.datasources.WriteJobStatsTracker
import org.apache.spark.sql.execution.datasources.WriteTaskStats
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.TaskContext

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

    val extendedData = extendDataWithFileUUID(data, tableChanges)

    // Add the required Hudi metadata columns to the schema and create an extended schema
    // by appending them to the original schema fields.
    val newColumns = Seq(
      HoodieRecord.COMMIT_TIME_METADATA_FIELD,
      HoodieRecord.COMMIT_SEQNO_METADATA_FIELD,
      HoodieRecord.RECORD_KEY_METADATA_FIELD,
      HoodieRecord.PARTITION_PATH_METADATA_FIELD,
      HoodieRecord.FILENAME_METADATA_FIELD)
      .map(StructField(_, StringType, nullable = false))
    val hudiSchema = StructType(newColumns ++ schema.fields)

    // This function adds the columns required by Hudi to the given row,
    // as specified in the extended schema, and returns a tuple containing
    // the modified row and the corresponding target filename.
    val processRow = getProcessRow(schema, commitTime)

    val filesAndStats =
      doWrite(tableId, hudiSchema, extendedData, tableChanges, statsTrackers, Some(processRow))
    val stats = filesAndStats.map(_._2)
    processStats(stats, statsTrackers)
    filesAndStats.map(_._1)
  }

  private def getProcessRow(schema: StructType, commitTime: String): ProcessRows = {
    (row: InternalRow, fileUUID: String) =>
      {
        val partitionId = TaskContext.getPartitionId()
        val stageId = TaskContext.get.stageId()
        val taskAttemptId = TaskContext.get.taskAttemptId()

        val writeToken = FSUtils.makeWriteToken(partitionId, stageId, taskAttemptId)
        val fileName = FSUtils.makeBaseFileName(commitTime, writeToken, fileUUID, ".parquet")

        val groupID = 0
        val rowID = 0

        val commitSeqno = s"$commitTime-seq-$groupID-$rowID"
        val recordKey = s"$commitTime-key-$groupID-$rowID"

        // Construct the processed row with the necessary columns
        val processedRow = InternalRow.fromSeq(
          Seq(
            UTF8String.fromString(commitTime), // _hoodie_commit_time
            UTF8String.fromString(commitSeqno), // _hoodie_commit_seqno
            UTF8String.fromString(recordKey), // _hoodie_record_key
            UTF8String.fromString(""), // _hoodie_partition_path
            UTF8String.fromString(fileName) // _hoodie_file_name
          ) ++ row.toSeq(schema))

        // Return processed row along with the file name
        (processedRow, fileName)
      }
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

}
