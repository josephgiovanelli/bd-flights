package org.queue.bd

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD

object RDDUtils {
  implicit class RichRDD[T](rDD: RDD[T]) {

    def overwrite(path: String): Unit = {

      def _deleteFolder(path: String): Unit = {

        try {
          FileSystem.get(new java.net.URI(path), new Configuration())
            .delete(new Path(path), true)
        } catch {
          case _: Throwable =>
        }
      }

      _deleteFolder(path)
      rDD.saveAsTextFile(path)
    }
  }
}

