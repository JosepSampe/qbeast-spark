package io.qbeast.spark.hudi

import io.qbeast.core.model._
import io.qbeast.spark.index.IndexStatusBuilder
import io.qbeast.spark.utils.MetadataConfig
import io.qbeast.IISeq
import org.apache.avro.Schema
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.storage.StoragePath
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

case class HudiQbeastSnapshot(tableID: QTableID) extends QbeastSnapshot {

  private val spark = SparkSession.active

  private val jsc = new JavaSparkContext(spark.sparkContext)

  /**
   * The current state of the snapshot.
   *
   * @return
   */
  override def isInitial: Boolean = {
    val timeline = loadTimeline()
    val lastInstantOption = if (timeline.empty) None else Some(timeline.lastInstant.get)
    lastInstantOption.isEmpty
  }

  override lazy val schema: StructType = {
    val schemaString = metadataMap(HoodieCommitMetadata.SCHEMA_KEY)
    val avroSchema = new Schema.Parser().parse(schemaString)
    SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]
  }

  override lazy val allFilesCount: Long = {
    val timeline = loadTimeline()
    val lastInstant = timeline.filterCompletedInstants.lastInstant()
    if (lastInstant.isPresent) {
      val commitMetadataBytes = timeline.getInstantDetails(lastInstant.get()).get()
      val commitMetadata =
        HoodieCommitMetadata.fromBytes(commitMetadataBytes, classOf[HoodieCommitMetadata])
      val basePath = new StoragePath(tableID.id)
      commitMetadata.getFileIdAndFullPaths(basePath).keySet().size()
    } else {
      0
    }
  }

  private val metadataMap: Map[String, String] = {
    val timeline = loadTimeline()
    val lastInstant = timeline.filterCompletedInstants.lastInstant()
    if (lastInstant.isPresent) {
      val commitMetadataBytes = timeline.getInstantDetails(lastInstant.get()).get()
      val commitMetadata =
        HoodieCommitMetadata.fromBytes(commitMetadataBytes, classOf[HoodieCommitMetadata])
      commitMetadata.getExtraMetadata.asScala.toMap
    } else {
      Map.empty
    }
  }

  override def loadProperties: Map[String, String] = {
    // Check if it is required to load more props from metadataMap
    loadMetaClient().getTableConfig.getProps.asScala.toMap
  }

  override def loadDescription: String = s"Hudi table snapshot at ${tableID.id}"

  // Revision map based on metadata properties
  private val revisionsMap: Map[RevisionID, Revision] = {
    val revisionsMetadata = if (metadataMap.contains(MetadataConfig.revisions)) {
      mapper
        .readTree(metadataMap(MetadataConfig.revisions))
        .fields()
        .asScala
        .map(entry => entry.getKey -> entry.getValue.asText())
        .toMap
    } else {
      Map.empty
    }
    val listRevisions = revisionsMetadata.filterKeys(_.startsWith(MetadataConfig.revision))
    listRevisions.map { case (key, json) =>
      val revisionID = key.split('.').last.toLong
      val revision = mapper.readValue[Revision](json, classOf[Revision])
      (revisionID, revision)
    }
  }

  private def loadMetaClient(): HoodieTableMetaClient = {
    HoodieTableMetaClient
      .builder()
      .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
      .setBasePath(tableID.id)
      .build()
  }

  private def loadTimeline(): HoodieTimeline = {
    loadMetaClient().getActiveTimeline.getCommitTimeline.filterCompletedInstants
  }

  private val lastRevisionID: RevisionID =
    metadataMap.getOrElse(MetadataConfig.lastRevisionID, "-1").toLong

  private def getRevision(revisionID: RevisionID): Revision = {
    revisionsMap.getOrElse(
      revisionID,
      throw AnalysisExceptionFactory.create(s"No space revision available with $revisionID"))
  }

  override def existsRevision(revisionID: RevisionID): Boolean = revisionsMap.contains(revisionID)

  override def loadLatestIndexStatus: IndexStatus = loadIndexStatus(lastRevisionID)

  override def loadIndexStatus(revisionID: RevisionID): IndexStatus = {
    val revision = getRevision(revisionID)
    new IndexStatusBuilder(this, revision).build()
  }

  override def loadLatestIndexFiles: Dataset[IndexFile] = loadIndexFiles(lastRevisionID)

  override def loadIndexFiles(revisionID: RevisionID): Dataset[IndexFile] = {
    val dimensionCount = loadRevision(revisionID).transformations.size
    val indexFilesBuffer = ListBuffer[IndexFile]()

    val metaClient = loadMetaClient()
    val timeline: HoodieTimeline =
      metaClient.getActiveTimeline.getCommitTimeline.filterCompletedInstants

    timeline.getInstants.iterator().asScala.foreach { instant =>
      val commitMetadataBytes = metaClient.getActiveTimeline
        .getInstantDetails(instant)
        .get()
      val commitMetadata =
        HoodieCommitMetadata.fromBytes(commitMetadataBytes, classOf[HoodieCommitMetadata])
      val indexFiles = HudiQbeastFileUtils.fromCommitFile(dimensionCount)(commitMetadata)
      indexFilesBuffer ++= indexFiles
    }

    import spark.implicits._
    val indexFilesDataset: Dataset[IndexFile] = spark.createDataset(indexFilesBuffer.toList)
    indexFilesDataset
  }

  override def loadAllRevisions: IISeq[Revision] = revisionsMap.values.toVector

  override def loadLatestRevision: Revision = getRevision(lastRevisionID)

  override def loadRevision(revisionID: RevisionID): Revision = getRevision(revisionID)

  override def loadRevisionAt(timestamp: Long): Revision = {
    val candidateRevisions = revisionsMap.values.filter(_.timestamp <= timestamp)
    if (candidateRevisions.nonEmpty) candidateRevisions.maxBy(_.timestamp)
    else {
      throw AnalysisExceptionFactory.create(s"No space revision available before $timestamp")
    }
  }

  override def loadDataframeFromIndexFiles(indexFile: Dataset[IndexFile]): DataFrame = {
    import indexFile.sparkSession.implicits._
    val paths = indexFile.map(file => new Path(tableID.id, file.path).toString).collect()
    indexFile.sparkSession.read
      .schema(schema)
      .load(paths: _*)
  }

}
