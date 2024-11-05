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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.BasicWriteTaskStats
import org.apache.spark.sql.execution.datasources.WriteJobStatsTracker
import org.apache.spark.sql.execution.datasources.WriteTaskStats
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame

/**
 * Delta implementation of DataWriter that applies rollup to compact the files.
 */
object HudiRollupDataWriter extends RollupDataWriter {

  override type GetCubeMaxWeight = CubeId => Weight
  override type Extract = InternalRow => (InternalRow, Weight, CubeId, CubeId)
  override type WriteRows = Iterator[InternalRow] => Iterator[(IndexFile, TaskStats)]

  override def write(
      tableId: QTableID,
      schema: StructType,
      data: DataFrame,
      tableChanges: TableChanges): IISeq[IndexFile] = {

    val statsTrackers = StatsTracker.getStatsTrackers

    val filesAndStats =
      internalWrite(tableId, schema, data, tableChanges, statsTrackers)
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

}
