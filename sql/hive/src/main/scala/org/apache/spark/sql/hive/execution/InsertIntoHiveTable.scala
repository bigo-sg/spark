/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.execution

import java.io.IOException
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.ql.ErrorMsg
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.InputFormat
import org.apache.spark.SparkException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.{EmptyRDD, HadoopRDD, RDD, UnionRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, ExternalCatalog}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast, Literal, SpecificInternalRow}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.CommandUtils
import org.apache.spark.sql.hive.HadoopTableReader
import org.apache.spark.sql.hive.HiveShim.{ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.util.{SerializableConfiguration, Utils}

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map => MutableMap}

/**
  * Command for writing data out to a Hive table.
  *
  * This class is mostly a mess, for legacy reasons (since it evolved in organic ways and had to
  * follow Hive's internal implementations closely, which itself was a mess too). Please don't
  * blame Reynold for this! He was just moving code around!
  *
  * In the future we should converge the write path for Hive with the normal data source write path,
  * as defined in `org.apache.spark.sql.execution.datasources.FileFormatWriter`.
  *
  * @param table                the metadata of the table.
  * @param partition            a map from the partition key to the partition value (optional). If the partition
  *                             value is optional, dynamic partition insert will be performed.
  *                             As an example, `INSERT INTO tbl PARTITION (a=1, b=2) AS ...` would have
  *
  *                             {{{
  *                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              Map('a' -> Some('1'), 'b' -> Some('2'))
  *                             }}}
  *
  *                             and `INSERT INTO tbl PARTITION (a=1, b) AS ...`
  *                             would have
  *
  *                             {{{
  *                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              Map('a' -> Some('1'), 'b' -> None)
  *                             }}}.
  * @param query                the logical plan representing data to write to.
  * @param overwrite            overwrite existing table or partitions.
  * @param ifPartitionNotExists If true, only write if the partition does not exist.
  *                             Only valid for static partitions.
  */
case class InsertIntoHiveTable(
                                table: CatalogTable,
                                partition: Map[String, Option[String]],
                                query: LogicalPlan,
                                overwrite: Boolean,
                                ifPartitionNotExists: Boolean,
                                outputColumnNames: Seq[String]) extends SaveAsHiveFile {


  var avgConditionSize = 0l
  var avgOutputSize = 0l
  var mergeEnabled = false
  var orcmergeEnabled=false

  /**
    * Inserts all the rows in the table into Hive.  Row objects are properly serialized with the
    * `org.apache.hadoop.hive.serde2.SerDe` and the
    * `org.apache.hadoop.mapred.OutputFormat` provided by the table definition.
    */
  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    val externalCatalog = sparkSession.sharedState.externalCatalog
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    mergeEnabled = sparkSession.conf.get("spark.sql.merge.output.enabled", "false").toBoolean
    avgConditionSize = sparkSession.conf.get("spark.sql.merge.output.avgcond", "128000000").toInt
    avgOutputSize = sparkSession.conf.get("spark.sql.merge.output.avgoutput", "256000000").toInt
    orcmergeEnabled =sparkSession.conf.get("spark.sql.fastmerge.orc.enabled","false").toBoolean

    val hiveQlTable = HiveClientImpl.toHiveTable(table)
    // Have to pass the TableDesc object to RDD.mapPartitions and then instantiate new serializer
    // instances within the closure, since Serializer is not serializable while TableDesc is.
    val tableDesc = new TableDesc(
      hiveQlTable.getInputFormatClass,
      // The class of table should be org.apache.hadoop.hive.ql.metadata.Table because
      // getOutputFormatClass will use HiveFileFormatUtils.getOutputFormatSubstitute to
      // substitute some output formats, e.g. substituting SequenceFileOutputFormat to
      // HiveSequenceFileOutputFormat.
      hiveQlTable.getOutputFormatClass,
      hiveQlTable.getMetadata
    )
    val tableLocation = hiveQlTable.getDataLocation
    val tmpLocation = getExternalTmpPath(sparkSession, hadoopConf, tableLocation)
    logInfo("tmp location is " + tmpLocation)

    try {
      processInsert(sparkSession, externalCatalog, hadoopConf, tableDesc, tmpLocation, child)
    } finally {
      // Attempt to delete the staging directory and the inclusive files. If failed, the files are
      // expected to be dropped at the normal termination of VM since deleteOnExit is used.
      deleteExternalTmpPath(hadoopConf)
    }

    // un-cache this table.
    sparkSession.catalog.uncacheTable(table.identifier.quotedString)
    sparkSession.sessionState.catalog.refreshTable(table.identifier)

    CommandUtils.updateTableStats(sparkSession, table)

    // It would be nice to just return the childRdd unchanged so insert operations could be chained,
    // however for now we return an empty list to simplify compatibility checks with hive, which
    // does not return anything for insert operations.
    // TODO: implement hive compatibility as rules.
    Seq.empty[Row]
  }

  private def processInsert(
                             sparkSession: SparkSession,
                             externalCatalog: ExternalCatalog,
                             hadoopConf: Configuration,
                             tableDesc: TableDesc,
                             tmpLocation: Path,
                             child: SparkPlan): Unit = {
    var tmpLocation2: Path = tmpLocation
    val catalogTable = table

    implicit class SchemaAttribute(f: StructField) {
      def toAttribute: AttributeReference = AttributeReference(
        f.name,
        f.dataType,
        // Since data can be dumped in randomly with no validation, everything is nullable.
        nullable = true
      )(qualifier = List(catalogTable.identifier.table))
    }

    val dattributes = catalogTable.schema.filter { c => !catalogTable.partitionColumnNames.contains(c.name) }
      .map(_.toAttribute)
    val datacs = dattributes.map(c => c.name).mkString(",")
    logInfo("catalog table data column: " + datacs)
    val part_attributes = catalogTable.partitionSchema.map(_.toAttribute)
    val partcs = part_attributes.map(c => c.name).mkString(",")
    logInfo(msg = "catalog table partition: " + partcs)
    logInfo("output columns: " + outputColumnNames.mkString(","))

    def getPathSize(inpFs: FileSystem, dirPath: Path): AverageSize = {
      val error = new AverageSize(-1, -1)
      try {
        val fStats = inpFs.listStatus(dirPath)
        var totalSz: Long = 0L
        var numFiles: Int = 0

        for (fStat <- fStats) {
          if (fStat.isDirectory()) {
            val avgSzDir = getPathSize(inpFs, fStat.getPath)
            if (avgSzDir.totalSize < 0) {
              return error
            }
            totalSz += avgSzDir.totalSize
            numFiles += avgSzDir.numFiles
          } else {
            if (!fStat.getPath.toString.endsWith("_SUCCESS")) {
              totalSz += fStat.getLen
              numFiles += 1
            }
          }
        }
        new AverageSize(totalSz, numFiles)
      } catch {
        case _: IOException => error
      }
    }

    // get all the dynamic partition path and it's file size
    def getTmpDynamicPartPathInfo(tmpPath: Path,
                                  conf: Configuration): MutableMap[String, AverageSize] = {
      val fs = tmpPath.getFileSystem(conf)
      val fStatus = fs.listStatus(tmpPath)
      val partPathInfo = MutableMap[String, AverageSize]()

      for (fStat <- fStatus) {
        if (fStat.isDirectory) {
          logInfo("[TmpDynamicPartPathInfo] path: " + fStat.getPath.toString)
          if (!hasFile(fStat.getPath, conf)) {
            partPathInfo ++= getTmpDynamicPartPathInfo(fStat.getPath, conf)
          } else {
            val avgSize = getPathSize(fs, fStat.getPath)
            logInfo("pathSizeMap: (" + fStat.getPath.toString + " -> " + avgSize.totalSize + ")")
            partPathInfo += (fStat.getPath.toString -> avgSize)
          }
        }
      }
      partPathInfo
    }

    def hasFile(path: Path, conf: Configuration): Boolean = {
      val fs = path.getFileSystem(conf)
      val fStatus = fs.listStatus(path)
      for (fStat <- fStatus) {
        if (fStat.isFile) {
          return true
        }
      }
      false
    }

    def getExternalMergeTmpPath(tempPath: Path, hadoopConf: Configuration): Path = {
      val fs = tempPath.getFileSystem(hadoopConf)
      val tempMergePath = tempPath.toString.replace("-ext-10000", "-ext-10000-merge")
      val dir: Path = fs.makeQualified(new Path(tempMergePath))
      logInfo("Created temp merging dir = " + dir + " for path = " + tempPath)
      try {
        if (!FileUtils.mkdir(fs, dir, true, hadoopConf)) {
          throw new IllegalStateException("Cannot create staging directory  '" + dir.toString + "'")
        }
        fs.deleteOnExit(dir)
      }
      catch {
        case e: IOException =>
          throw new RuntimeException(
            "Cannot create temp merging directory '" + dir.toString + "': " + e.getMessage, e)

      }

      return dir
    }

    def getRePartitionNum(path: Path, conf: Configuration): (Int,Long) = {
      var rePartitionNum = -1
      val inpFs = path.getFileSystem(conf)
      val totSize = getPathSize(inpFs, path)
      logInfo("static partition totalsize of path: " + path.toString + " is: "
        + totSize.totalSize + "; numFiles is: " + totSize.numFiles)
      rePartitionNum = computeMergePartitionNum(totSize)
      (rePartitionNum, totSize.getAverageSize)
    }

    def computeMergePartitionNum(totsize: AverageSize): Int = {
      logInfo("[computeMergePartitionNum] totsize -> " + totsize.toString)
      var partitionNum = -1
      if (totsize.numFiles <= 1) {
        partitionNum = -1
      } else {
        if (totsize.getAverageSize > avgConditionSize) {
          partitionNum = -1
        } else {
          partitionNum = Math.ceil(totsize.totalSize.toDouble / avgOutputSize).toInt
        }
      }
      partitionNum
    }

    case class DynamicPartMergeRule(var plist: ArrayBuffer[(String, Int)]) {
      override def toString: String = {
        plist.map { case (path, repartition) => s"$path: $repartition" }.mkString("\n")
      }
    }

    def generateDynamicMergeRule(path: Path, conf: Configuration, directRenamePathList: ListBuffer[String]): DynamicPartMergeRule = {
      val tmpDynamicPartInfos = getTmpDynamicPartPathInfo(path, conf)
      logInfo("[generateDynamicMergeRule] tmp partInfo size: " + tmpDynamicPartInfos.size)
      val mergeRule = DynamicPartMergeRule(new ArrayBuffer[(String, Int)]())
      for (part <- tmpDynamicPartInfos) {
        if (part._2.numFiles <= 1 || part._2.getAverageSize > avgConditionSize) {
          logInfo("direct rename " + part._1 + " number of files " + part._2.numFiles)
          directRenamePathList += part._1
        } else {
          val targetPart = computeMergePartitionNum(part._2)
          mergeRule.plist += ((part._1, targetPart.toInt))
          logInfo(s"add merge path " + part._1 + s" and target partition $targetPart")
        }
      }
      mergeRule
    }

    def unionRdds(rdds: List[RDD[InternalRow]]): RDD[InternalRow] = {
      // Even if we don't use any partitions, we still need an empty RDD
      if (rdds.size == 0) {
        new EmptyRDD[InternalRow](sparkSession.sparkContext)
      } else {
        new UnionRDD(rdds(0).context, rdds)
      }
    }

    val fileSinkConf = new FileSinkDesc(tmpLocation.toString, tableDesc, false)

    val numDynamicPartitions = partition.values.count(_.isEmpty)
    val numStaticPartitions = partition.values.count(_.nonEmpty)
    val partitionSpec = partition.map {
      case (key, Some(value)) => key -> value
      case (key, None) => key -> ""
    }

    logInfo("partition spec key size is " + partitionSpec.size)
    // All partition column names in the format of "<column name 1>/<column name 2>/..."
    val partitionColumns = fileSinkConf.getTableInfo.getProperties.getProperty("partition_columns")
    val partitionColumnNames = Option(partitionColumns).map(_.split("/")).getOrElse(Array.empty)

    // By this time, the partition map must match the table's partition columns
    if (partitionColumnNames.toSet != partition.keySet) {
      throw new SparkException(
        s"""Requested partitioning does not match the ${table.identifier.table} table:
           |Requested partitions: ${partition.keys.mkString(",")}
           |Table partitions: ${table.partitionColumnNames.mkString(",")}""".stripMargin)
    }

    // Validate partition spec if there exist any dynamic partitions
    if (numDynamicPartitions > 0) {
      // Report error if dynamic partitioning is not enabled
      if (!hadoopConf.get("hive.exec.dynamic.partition", "true").toBoolean) {
        throw new SparkException(ErrorMsg.DYNAMIC_PARTITION_DISABLED.getMsg)
      }

      // Report error if dynamic partition strict mode is on but no static partition is found
      if (numStaticPartitions == 0 &&
        hadoopConf.get("hive.exec.dynamic.partition.mode", "strict").equalsIgnoreCase("strict")) {
        throw new SparkException(ErrorMsg.DYNAMIC_PARTITION_STRICT_MODE.getMsg)
      }

      // Report error if any static partition appears after a dynamic partition
      val isDynamic = partitionColumnNames.map(partitionSpec(_).isEmpty)
      if (isDynamic.init.zip(isDynamic.tail).contains((true, false))) {
        throw new AnalysisException(ErrorMsg.PARTITION_DYN_STA_ORDER.getMsg)
      }
    }

    table.bucketSpec match {
      case Some(bucketSpec) =>
        // Writes to bucketed hive tables are allowed only if user does not care about maintaining
        // table's bucketing ie. both "hive.enforce.bucketing" and "hive.enforce.sorting" are
        // set to false
        val enforceBucketingConfig = "hive.enforce.bucketing"
        val enforceSortingConfig = "hive.enforce.sorting"

        val message = s"Output Hive table ${table.identifier} is bucketed but Spark " +
          "currently does NOT populate bucketed output which is compatible with Hive."

        if (hadoopConf.get(enforceBucketingConfig, "true").toBoolean ||
          hadoopConf.get(enforceSortingConfig, "true").toBoolean) {
          throw new AnalysisException(message)
        } else {
          logWarning(message + s" Inserting data anyways since both $enforceBucketingConfig and " +
            s"$enforceSortingConfig are set to false.")
        }
      case _ => // do nothing since table has no bucketing
    }

    val partitionAttributes = partitionColumnNames.takeRight(numDynamicPartitions).map { name =>
      query.resolve(name :: Nil, sparkSession.sessionState.analyzer.resolver).getOrElse {
        throw new AnalysisException(
          s"Unable to resolve $name given [${query.output.map(_.name).mkString(", ")}]")
      }.asInstanceOf[Attribute]
    }

    saveAsHiveFile(
      sparkSession = sparkSession,
      plan = child,
      hadoopConf = hadoopConf,
      fileSinkConf = fileSinkConf,
      outputLocation = tmpLocation.toString,
      partitionAttributes = partitionAttributes)

    if(tableDesc.getOutputFileFormatClassName.toLowerCase.contains("parquet")){
      logInfo("output parquet format, set hive.io.file.readcolumn.ids")
      val columnIndexSeq = Range(0,catalogTable.dataSchema.size).mkString(",")
      hadoopConf.set("hive.io.file.readcolumn.ids",columnIndexSeq)
    }
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    def createHadoopRdd(tableDesc: TableDesc,
                        path: String,
                        inputFormatClass: Class[InputFormat[Writable, Writable]], minSplits: Int): RDD[Writable] = {

      val initializeJobConfFunc = HadoopTableReader.initializeLocalJobConfFunc(path, tableDesc) _

      val rdd = new HadoopRDD(
        sparkSession.sparkContext,
        broadcastedHadoopConf.asInstanceOf[Broadcast[SerializableConfiguration]],
        Some(initializeJobConfFunc),
        inputFormatClass,
        classOf[Writable],
        classOf[Writable],
        minSplits)

      // Only take the value (skip the key) because Hive works only with values.
      rdd.map(_._2)
    }

    def makeMergedRddForPartition(inputPathStr: Path, dynamicPartKey: Seq[Attribute], ifc: Class[InputFormat[Writable, Writable]]): RDD[InternalRow] = {
      val mutableRow = new SpecificInternalRow(outputColumns.map(_.dataType))
      val (partitionKeyAttrs, nonPartitionKeyAttrs) = outputColumns.zipWithIndex.partition {
        case (attr, _) => dynamicPartKey.contains(attr)
      }
      val part2val = getPartValue(inputPathStr.toString, dynamicPartKey.map(_.name))

      partitionKeyAttrs.foreach { case (attr, ordinal) =>
        mutableRow(ordinal) = Cast(Literal(part2val(attr.name)), attr.dataType).eval(null)
      }

      val tprop = tableDesc.getProperties
      val deSerRdd = createHadoopRdd(tableDesc, inputPathStr.toString, ifc, minSplitsPerRDD(hadoopConf, sparkSession)).mapPartitions {
        iter =>
          val hconf = broadcastedHadoopConf.value.value
          val deserializer = tableDesc.getDeserializer(hconf)
          val props = new Properties(tprop)
          deserializer.initialize(hconf, props)

          val tableSerDe = tableDesc.getDeserializerClass.newInstance()
          tableSerDe.initialize(hconf, tableDesc.getProperties)
          HadoopTableReader.fillObject(iter, deserializer, nonPartitionKeyAttrs,
            mutableRow, tableSerDe)
      }
      deSerRdd
    }

    def makeMergedRDDForPartitionedTable(rule: DynamicPartMergeRule, dynamicPartKey: Seq[Attribute]): RDD[InternalRow] = {
      val ifc = tableDesc.getInputFileFormatClass.asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
      val rdds = rule.plist.map { case (path, repartnum) =>
        logInfo(s"[makeMergedRDDForPartitionedTable] create rdd for $path repartition $repartnum")
        makeMergedRddForPartition(new Path(path), dynamicPartKey, ifc).map(row => row.copy()).coalesce(repartnum)
      }.toList
      unionRdds(rdds)
    }

    def makeMergedRDDForTable(path: Path, rePartitionNum: Int): RDD[InternalRow] = {
      val ifc = tableDesc.getInputFileFormatClass.asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
      val hadoopRDD = createHadoopRdd(tableDesc, path.toString, ifc, minSplitsPerRDD(hadoopConf, sparkSession))
      val attributes = catalogTable.schema.filter { c => !catalogTable.partitionColumnNames.contains(c.name) }
        .map(_.toAttribute)
      val attrsWithIndex = attributes.zipWithIndex
      val mutableRow = new SpecificInternalRow(attributes.map(_.dataType))
      val deserializerClass = Utils.classForName(tableDesc.getSerdeClassName).asInstanceOf[Class[Deserializer]]

      val deserializedHadoopRDD = hadoopRDD.mapPartitions { iter =>
        val hconf = broadcastedHadoopConf.value.value
        val deserializer = deserializerClass.newInstance()
        deserializer.initialize(hconf, tableDesc.getProperties)
        HadoopTableReader.fillObject(iter, deserializer, attrsWithIndex, mutableRow, deserializer)
      }

      deserializedHadoopRDD.map(row => row.copy()).coalesce(rePartitionNum)
    }

    def mergeFiles(path: Path, fileSinkConf: FileSinkDesc, directRenamePathList: ListBuffer[String]): Unit = {
      logInfo("[mergeFile] dirname in fileSinkConf is: " + fileSinkConf.dir)
      val numDynamicPartitions = partition.values.count(_.isEmpty)

      if (numDynamicPartitions > 0) {
        logInfo("[mergeFile] numDynamicPartitions -> " + numDynamicPartitions)
        val mergeRule = generateDynamicMergeRule(path, hadoopConf, directRenamePathList)
        logInfo(s"[mergeFile] merge candidate " + mergeRule.plist.size)
        if (mergeRule.plist.nonEmpty) {
          if(fileSinkConf.tableInfo.getOutputFileFormatClassName.toLowerCase.endsWith("orcoutputformat") && orcmergeEnabled){
            logInfo("fast merge orc dynamic part")
            OrcMergeUtil.mergeDynamicPartOrc(mergeRule.plist,broadcastedHadoopConf,sparkSession)
          }else {
            val rdd = makeMergedRDDForPartitionedTable(mergeRule, partitionAttributes)
            saveAsHiveFile2(sparkSession = sparkSession, rdd = rdd, hadoopConf = hadoopConf, fileSinkConf = fileSinkConf,
              outputLocation = fileSinkConf.dir, partitionAttributes = partitionAttributes)
          }
          logInfo("[mergeFile] merge dynamic partition finished")
        }
      } else {
        val (reParitionNum, avgsize) = getRePartitionNum(path, hadoopConf)
        logInfo(s"[mergeFile] static $path rePartionNum is: " + reParitionNum)
        if (reParitionNum > 0) {
          if(fileSinkConf.tableInfo.getOutputFileFormatClassName.toLowerCase.endsWith("orcoutputformat") && orcmergeEnabled
          && avgsize > 2*1024*1024){
            logInfo("fast merge orc static part")
            OrcMergeUtil.mergeStaticPartOrc(path,reParitionNum,broadcastedHadoopConf,sparkSession)
          }else {
            val rdd = makeMergedRDDForTable(path, reParitionNum)
            saveAsHiveFile2(sparkSession = sparkSession, rdd = rdd, hadoopConf = hadoopConf, fileSinkConf = fileSinkConf,
              outputLocation = fileSinkConf.dir, partitionAttributes = partitionAttributes)
          }
          logInfo("[mergeFile] merge static partition finished")
        } else {
          logInfo(s"direct rename $path")
          directRenamePathList += path.toString
        }
      }

    }

    if (mergeEnabled && avgOutputSize > avgConditionSize) {
      logInfo("enable merge files for " + tmpLocation)
      val directRenamePathList = new ListBuffer[String]
      val rollbackPathList = new ListBuffer[String]
      val fs = tmpLocation.getFileSystem(hadoopConf)
      try {
        val tmpMergeLocation = getExternalMergeTmpPath(tmpLocation, hadoopConf)
        fileSinkConf.dir = tmpMergeLocation.toString
        mergeFiles(tmpLocation, fileSinkConf, directRenamePathList)
        if (directRenamePathList.nonEmpty) {
          if (directRenamePathList.size == 1 && directRenamePathList.head.endsWith("ext-10000")) {
            logInfo("can not direct rename ext-10000, delete temp merge dir first")
            fs.delete(tmpMergeLocation, true)
          }
          directRenamePathList.foreach {
            path =>
              val destPath = path.replace("ext-10000", "ext-10000-merge")
              val destParent = new Path(destPath).getParent
              if(!fs.exists(destParent)){
                fs.mkdirs(destParent)
                logInfo("mkdir direct parent " + destParent)
              }
              if(!fs.rename(new Path(path), new Path(destPath))) throw new IOException(s"direct rename fail from $path to $destPath")
              rollbackPathList += destPath
              logInfo("direct rename [" + path + " to " + destPath + "]")
          }
        }
        tmpLocation2 = tmpMergeLocation
      } catch {
        case e: Throwable =>
          logError("merge error found ", e)
          if (rollbackPathList.nonEmpty) {
            rollbackPathList.foreach { dest =>
              fs.rename(new Path(dest), new Path(dest.replace("ext-10000-merge", "ext-10000")))
              logInfo("rollback path " + dest)
            }
          }
      }
    }

    if (partition.nonEmpty) {
      if (numDynamicPartitions > 0) {
        externalCatalog.loadDynamicPartitions(
          db = table.database,
          table = table.identifier.table,
          tmpLocation2.toString,
          partitionSpec,
          overwrite,
          numDynamicPartitions)
      } else {
        // scalastyle:off
        // ifNotExists is only valid with static partition, refer to
        // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-InsertingdataintoHiveTablesfromqueries
        // scalastyle:on
        val oldPart =
        externalCatalog.getPartitionOption(
          table.database,
          table.identifier.table,
          partitionSpec)

        var doHiveOverwrite = overwrite

        if (oldPart.isEmpty || !ifPartitionNotExists) {
          // SPARK-18107: Insert overwrite runs much slower than hive-client.
          // Newer Hive largely improves insert overwrite performance. As Spark uses older Hive
          // version and we may not want to catch up new Hive version every time. We delete the
          // Hive partition first and then load data file into the Hive partition.
/*          if (oldPart.nonEmpty && overwrite) {
            oldPart.get.storage.locationUri.foreach { uri =>
              val partitionPath = new Path(uri)
              val fs = partitionPath.getFileSystem(hadoopConf)
              if (fs.exists(partitionPath)) {
                if (!fs.delete(partitionPath, true)) {
                  throw new RuntimeException(
                    "Cannot remove partition directory '" + partitionPath.toString)
                }
                // Don't let Hive do overwrite operation since it is slower.
                doHiveOverwrite = false
              }
            }
          }*/

          // inheritTableSpecs is set to true. It should be set to false for an IMPORT query
          // which is currently considered as a Hive native command.
          val inheritTableSpecs = true
          externalCatalog.loadPartition(
            table.database,
            table.identifier.table,
            tmpLocation2.toString,
            partitionSpec,
            isOverwrite = doHiveOverwrite,
            inheritTableSpecs = inheritTableSpecs,
            isSrcLocal = false)
        }
      }
    } else {
      externalCatalog.loadTable(
        table.database,
        table.identifier.table,
        tmpLocation2.toString, // TODO: URI
        overwrite,
        isSrcLocal = false)
    }
  }

  private class AverageSize(var totalSize: Long, var numFiles: Int) {
    def getAverageSize: Long = {
      if (numFiles != 0) {
        totalSize / numFiles
      } else {
        0
      }
    }

    override def toString: String = "{totalSize: " + totalSize + ", numFiles: " + numFiles + "}"
  }

  private def getPartValue(partPath: String, partCols: Seq[String]): Map[String, String] = {
    val splits = partPath.split("/").toList
    val splitSize = splits.size
    splits.slice(splitSize - partCols.size, splitSize).map {
      part =>
        val parts = part.split("=")
        if (partCols.contains(parts(0))) {
          logInfo("path value:" + parts(0) + "->" + parts(1))
          (parts(0), parts(1))
        } else {
          throw new IOException("not found the part key in dynamic cols " + partCols.mkString(","))
        }
    }.toMap
  }

  private def minSplitsPerRDD(conf: Configuration, session: SparkSession): Int = {
    if (session.sparkContext.isLocal) {
      0
    } else {
      math.max(conf.getInt("mapred.map.tasks", 1),
        session.sparkContext.defaultMinPartitions)
    }
  }

}
