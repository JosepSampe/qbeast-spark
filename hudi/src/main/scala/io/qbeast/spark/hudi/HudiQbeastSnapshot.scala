package io.qbeast.spark.hudi

import io.qbeast.core.model._
import io.qbeast.spark.index.IndexStatusBuilder
import io.qbeast.spark.utils.MetadataConfig
import io.qbeast.IISeq
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

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
    val metaClient = loadMetaClient(tableID)
    val timeline = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants
    val lastInstantOption = if (timeline.empty) None else Some(timeline.lastInstant.get)
    lastInstantOption.isEmpty
  }

  override lazy val schema: StructType = {
    spark.read.format("hudi").load(tableID.id).schema
  }

  override lazy val allFilesCount: Long = {
    spark.read.format("hudi").load(tableID.id).count()
  }

  private val metadataMap: Map[String, String] = {
    loadMetaClient(tableID).getTableConfig.getProps.asScala.toMap
  }

  override def loadProperties: Map[String, String] = metadataMap

  override def loadDescription: String = "Hudi table snapshot"

  // Revision map based on metadata properties
  private val revisionsMap: Map[RevisionID, Revision] = {
    val listRevisions = metadataMap.filterKeys(_.startsWith(MetadataConfig.revision))
    listRevisions.map { case (key, json) =>
      val revisionID = key.split('.').last.toLong
      val revision = mapper.readValue[Revision](json, classOf[Revision])
      (revisionID, revision)
    }
  }

  private def loadMetaClient(tableID: QTableID): HoodieTableMetaClient = {
    HoodieTableMetaClient
      .builder()
      .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
      .setBasePath(tableID.id)
      .build()
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
    // val dimensionCount = loadRevision(revisionID).transformations.size
    val addFiles = loadFilesForRevision(revisionID)
    println(addFiles)
    // addFiles.map(HudiQbeastFileUtils.fromHudiFile(dimensionCount))
    import spark.implicits._
    spark.emptyDataset[IndexFile]
  }

  private def loadFilesForRevision(revisionID: RevisionID): Dataset[String] = {
    val metaClient = loadMetaClient(tableID)
    val timeline = metaClient.getActiveTimeline.getCommitTimeline.filterCompletedInstants
    val lastCommitInstant = timeline.lastInstant().get()

    val commitMetadataBytes = metaClient.getActiveTimeline
      .getInstantDetails(lastCommitInstant)
      .get()
    val commitMetadata =
      HoodieCommitMetadata.fromBytes(commitMetadataBytes, classOf[HoodieCommitMetadata])

    println(commitMetadata)

    import spark.implicits._
    spark.emptyDataset[String]
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
