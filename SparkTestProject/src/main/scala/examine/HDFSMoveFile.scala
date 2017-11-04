package examine

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ListBuffer

/**
  * Created by gg on 17-11-4.
  */
object HDFSMoveFile {
  def main(args: Array[String]): Unit = {
    val HDFSURL="hdfs://192.168.2.101:9000";
    val hdfs = FileSystem.get(URI.create(HDFSURL),new Configuration());
    //后期换成参数args(0)
    HDFSUtil.copyFiles(hdfs, "hdfs://192.168.2.101:9000/spark/emp/temp/201711112025/d=20171111/h=20")
   }
   println("执行完毕")
}
