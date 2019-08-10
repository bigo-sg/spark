package org.apache.spark.sql.hive.execution

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import java.io.IOException

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.hive.ql.io.orc.OrcProto.UserMetadataItem
import org.apache.hadoop.hive.ql.io.orc.CompressionKind
import org.apache.hadoop.hive.ql.io.orc.OrcFile
import org.apache.hadoop.hive.ql.io.orc.OrcFileKeyWrapper
import org.apache.hadoop.hive.ql.io.orc.OrcFileValueWrapper
import org.apache.hadoop.hive.ql.io.orc.Reader
import org.apache.hadoop.hive.ql.io.orc.ReaderImpl
import org.apache.hadoop.hive.ql.io.orc.Writer
import org.apache.hadoop.hive.ql.io.orc.OrcProto.StripeStatistics
import java.util.List

import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.hive.common.FileUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.UnionRDD
import org.apache.spark.util.SerializableConfiguration
import scala.util.control.Breaks._

object OrcMergeUtil extends Logging{

  def mergeDynamicPartOrc(plist:ArrayBuffer[(String,Int)], bconf:Broadcast[SerializableConfiguration], sparkSession: SparkSession): Unit ={
    val conf = bconf.value.value
    val fs = FileSystem.get(conf)
    plist.foreach{ case(p,_)=>
      val newp = new Path(p.toString().replace("ext-10000","ext-10000-merge"))
      if(!FileUtils.mkdir(fs,newp,true,conf)){
        throw new IllegalStateException("Cannot create staging directory " + newp)
      }
      logInfo("Created orc dynamic part merging dir = " +  newp)
    }

    val rdds = plist.map{ case(inpath, num)=>
      val rdd = sparkSession.sparkContext.makeRDD(generateFilesWithLoc(new Path(inpath),fs)).coalesce(num).mapPartitionsWithIndex{
        (index,iter) =>
          val paths = iter.toList
          Option((index, paths)).iterator
      }
      rdd
    }

    val unionRdd = new UnionRDD(sparkSession.sparkContext,rdds)

    unionRdd.foreach{ case(index, paths) =>
      val conf = bconf.value.value
      mergeFiles(paths,index,conf)
    }

  }

  def mergeStaticPartOrc(inpath: Path, num:Int, bconf:Broadcast[SerializableConfiguration], sparkSession: SparkSession): Unit ={
    val conf = bconf.value.value
    val fs = FileSystem.get(conf)
    val rdd = sparkSession.sparkContext.makeRDD(generateFilesWithLoc(inpath,fs)).coalesce(num).mapPartitionsWithIndex{
      (index,iter) =>
        val paths = iter.toList
        Option((index, paths)).iterator
    }
    rdd.foreach{ case(index, paths) =>
      val conf = bconf.value.value
      mergeFiles(paths,index,conf)
    }

  }

  def generateFilesWithLoc(path:Path,fs:FileSystem):ListBuffer[(String,Seq[String])]={
    val iter = fs.listLocatedStatus(path)
    val filesStatus = new ListBuffer[LocatedFileStatus]
    while (iter.hasNext()) {
      filesStatus += iter.next
    }
    filesStatus.map { f =>
      val path = f.getPath.toString
      var hosts = Seq[String]()
      if (f.getBlockLocations.size > 0) {
        hosts = f.getBlockLocations.apply(0).getHosts.toSeq
      }
      (path, hosts)
    }
  }

  def mergeFiles(paths: List[String], index:Int, conf: Configuration) {
    var columnCount: Int = 0
    var compression: CompressionKind = null
    var compressBuffSize: Long = 0
    var version: OrcFile.Version = null
    var rowIndexStride: Int = 0
    var outfile:Path = null

    val fs = FileSystem.get(conf)
    var writer: Writer = null
    var reader: Reader = null
    val usermetaList = new ListBuffer[UserMetadataItem]

    for (p <- paths) {
      logInfo("begin merge one file " + p)
      val path = new Path(p)
      try {
        reader = OrcFile.createReader(path, OrcFile.readerOptions(conf).filesystem(fs))
      }catch {
        case e:Exception =>
          throw new IOException(s"read file meta $p fail",e)
      }
      val strinfos = reader.getStripes().iterator.toList
      //val stripeStatistics = reader.asInstanceOf[ReaderImpl].getOrcProtoStripeStatistics
      val method = classOf[ReaderImpl].getDeclaredMethod("getOrcProtoStripeStatistics")
      method.setAccessible(true)
      val stripeStatistics = method.invoke(reader.asInstanceOf[ReaderImpl]).asInstanceOf[List[StripeStatistics]]
      if ((stripeStatistics == null || stripeStatistics.isEmpty()) && reader.getNumberOfRows() > 0) {
        throw new IOException("stripe stat missing " + path)
      }
      breakable {
        if (reader.getNumberOfRows == 0) {
          println("found empty orc files " + path)
          break
        }

        val ssinfo = strinfos.zip(stripeStatistics)
        val keyWrapper = new OrcFileKeyWrapper
        keyWrapper.setInputPath(path)
        keyWrapper.setCompression(reader.getCompression)
        keyWrapper.setCompressBufferSize(reader.getCompressionSize);
        keyWrapper.setVersion(reader.getFileVersion);
        keyWrapper.setRowIndexStride(reader.getRowIndexStride);
        keyWrapper.setTypes(reader.getTypes);
        val usermeta = reader.asInstanceOf[ReaderImpl].getOrcProtoUserMetadata
        if (usermeta != null) {
          logInfo("usermeta found in " + path)
          usermetaList ++= usermeta
        }

        if (writer == null) {
          outfile = new Path(path.getParent.toString.replace("ext-10000", "ext-10000-merge"), "merge" + index + ".orc")
          logInfo("outfile created " + outfile)
          compression = keyWrapper.getCompression();
          compressBuffSize = keyWrapper.getCompressBufferSize();
          version = keyWrapper.getVersion();
          columnCount = keyWrapper.getTypes().get(0).getSubtypesCount();
          rowIndexStride = keyWrapper.getRowIndexStride();

          // block size and stripe size will be from config
          writer = OrcFile.createWriter(
            outfile,
            OrcFile.writerOptions(conf)
              .compress(compression)
              .version(version)
              .rowIndexStride(rowIndexStride).bufferSize(compressBuffSize.toInt)
              .inspector(reader.getObjectInspector()));
        }

        if (!checkCompatibility(keyWrapper)) {
          throw new IOException("check compatibility fail " + path)
        }

        ssinfo.foreach {
          case (si, ss) =>
            val v = new OrcFileValueWrapper
            v.setStripeInformation(si)
            v.setStripeStatistics(ss)
            mergeStripe(keyWrapper, v)
        }
        logInfo("merge finished one file " + path)
      }
    }
    writer.appendUserMetadata(usermetaList)
    writer.close

    def mergeStripe(k: OrcFileKeyWrapper, v: OrcFileValueWrapper) {
      var fdis: FSDataInputStream = null
      try {
        val buffer = new Array[Byte](v.getStripeInformation.getLength.toInt)
        fdis = fs.open(k.getInputPath)
        fdis.readFully(v.getStripeInformation.getOffset, buffer, 0,
          v.getStripeInformation.getLength.toInt)
        writer.appendStripe(buffer, 0, buffer.length, v.getStripeInformation,
          v.getStripeStatistics);
      } finally {
        if (fdis != null) {
          fdis.close
        }
      }
    }

    def checkCompatibility(k: OrcFileKeyWrapper): Boolean = {
      // check compatibility with subsequent files
      if ((k.getTypes().get(0).getSubtypesCount() != columnCount)) {
        logError("Incompatible ORC file merge! Column counts mismatch for " + k.getInputPath());
        return false;
      }

      if (!k.getCompression().equals(compression)) {
        logError("Incompatible ORC file merge! Compression codec mismatch for " + k.getInputPath());
        return false;
      }

      if (k.getCompressBufferSize() != compressBuffSize) {
        logError("Incompatible ORC file merge! Compression buffer size mismatch for " + k.getInputPath());
        return false;
      }

      if (!k.getVersion().equals(version)) {
        logError("Incompatible ORC file merge! Version mismatch for " + k.getInputPath());
        return false;
      }

      if (k.getRowIndexStride() != rowIndexStride) {
        logError("Incompatible ORC file merge! Row index stride mismatch for " + k.getInputPath());
        return false;
      }
      return true;
    }

  }

}
