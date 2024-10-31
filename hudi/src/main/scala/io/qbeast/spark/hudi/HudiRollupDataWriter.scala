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
    val revision = tableChanges.updatedRevision
    val dimensionCount = revision.transformations.length

    println(dimensionCount)
    println(tableChanges)

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

//  def write_test(
//      tableId: QTableID,
//      schema: StructType,
//      data: DataFrame,
//      tableChanges: TableChanges): IISeq[IndexFile] = {
//    val revision = tableChanges.updatedRevision
//    val revisionId = revision.revisionID
//    // val dimensionCount = revision.transformations.length
//
//    val spark = SparkSession.active
//    data.show(truncate = false)
//
//    // val statsTrackers = StatsTracker.getStatsTrackers
//
//    val rolledUp = rollupDataset(data, tableChanges)
//    val repData = rolledUp.repartition(col(QbeastColumns.cubeToRollupColumnName))
//    repData.show(truncate = false)
//
//    //    val jsc = new JavaSparkContext(spark.sparkContext)
//    //    val engineContext = new HoodieSparkEngineContext(jsc)
//    //    val writeConfig = HoodieWriteConfig
//    //      .newBuilder()
//    //      .withPath(tableId.id)
//    //      .forTable(hoodieCatalogTable.tableName)
//    //      .build()
//    //
//    //    val hoodieTable =
//    //      HoodieSparkTable.create(writeConfig, engineContext, hoodieCatalogTable.metaClient)
//
//    println(tableId.id)
//    val options = Map[String, String](
//      TABLE_TYPE.key -> "COPY_ON_WRITE",
//      PARTITIONPATH_FIELD.key -> QbeastColumns.cubeToRollupColumnName,
//      "hoodie.table.name" -> "hudi_qbeast_table",
//      "path" -> tableId.id,
//      "hoodie.cleaner.policy" -> "KEEP_LATEST_FILE_VERSIONS",
//      "hoodie.cleaner.fileversions.retained" -> "1",
//      "hoodie.metadata.record.index.enable" -> "true",
//      "hoodie.metadata.index.column.stats.enable" -> "true",
//      "hoodie.metadata.index.bloom.filter.enable" -> "true",
//      "hoodie.metadata.enable" -> "true",
//      "hoodie.clustering.inline" -> "false",
//      "hoodie.compact.inline" -> "false")
//
//    val result =
//      HoodieSparkSqlWriter.write(spark.sqlContext, SaveMode.Overwrite, options, repData)
//    println(result)
//
//    // DefaultSource
//
//    println("HUDI DONE")
//
//    val stats = QbeastStats(
//      numRecords = 1000L,
//      minValues = Map("column1" -> "0", "column2" -> "A"),
//      maxValues = Map("column1" -> "100", "column2" -> "Z"),
//      nullCount = Map("column1" -> "10", "column2" -> "5"))
//
//    val blocks = Seq(
//      Block(
//        filePath = "path/to/block/file",
//        cubeId = CubeId(
//          dimensionCount = 3,
//          depth = 5,
//          bitMask = Array(1L, 2L, 4L) // Example bitmask array
//        ),
//        minWeight = Weight(0.5),
//        maxWeight = Weight(1.5),
//        elementCount = 500L,
//        replicated = false)).toIndexedSeq
//
//    Seq(
//      IndexFile(
//        "path/to/file1",
//        12345L,
//        dataChange = true,
//        modificationTime = 1634567890000L,
//        revisionId,
//        blocks,
//        Option(stats))) toIndexedSeq
//
//  }

}
