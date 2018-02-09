package org.apache.spark.sql

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.execution.streaming.ManifestFileCommitProtocol

import scala.collection.mutable

/**
  * This extends ManifestFileCommitProtocol because Spark has special
  * behavior for that class in FileStreamSink. To keep that special behavior
  * we need to subclass instead of composing.
  *
  * The logic to move files is largely copied from
  * org.apache.spark.internal.io.HadoopMapReduceCommitProtocol.
  *
  * @param jobId
  * @param path
  */
class TempPathManifestFileCommitProtocol(jobId:String, path: String)
  extends ManifestFileCommitProtocol(jobId, path) {

  import org.apache.spark.sql.TempPathManifestFileCommitProtocol.tempSuffix

  // Track the list of temporary files, and their intended absolute paths
  @transient private var tempFiles: mutable.Map[String, String] = _

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    tempFiles = mutable.Map[String, String]()

    super.setupTask(taskContext)
  }

  override def newTaskTempFile(
                                taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    val superPathString = super.newTaskTempFile(taskContext, dir, ext)
    val superPath = new Path(superPathString)
    val name = superPath.getName
    val parent = superPath.getParent

    val tempPath = new Path(new Path(parent, tempSuffix), name).toString
    tempFiles(tempPath) = superPath.toString
    logDebug(s"Returning temporary path $tempPath")
    tempPath
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    logDebug(s"Moving staged task files $tempFiles")
    val fs = new Path(path).getFileSystem(taskContext.getConfiguration)
    for ((src, dst) <- tempFiles) {
      fs.rename(new Path(src), new Path(dst))
    }
    super.commitTask(taskContext)
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    for ((src, dst) <- tempFiles) {
      val tmp = new Path(src)
      val fs = tmp.getFileSystem(taskContext.getConfiguration)
      fs.delete(tmp, false)
      // since ManifestFileCommitProtocol doesn't try to delete files at all, we can do
      // it here for the destination files as well
      fs.delete(new Path(dst), false)
    }
    super.abortTask(taskContext)
  }
}

object TempPathManifestFileCommitProtocol {
  val tempSuffix = "_temporary"
}
