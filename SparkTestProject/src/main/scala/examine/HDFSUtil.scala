package examine

import java.io.{FileSystem => _, _}

import org.apache.hadoop.fs._

import scala.collection.mutable.ListBuffer

/**
  * Created by zls on 16-11-24.
  */
object HDFSUtil {
  var count=1;

  def isDir(hdfs : FileSystem, name : String) : Boolean = {
    hdfs.isDirectory(new Path(name))
  }
  def isDir(hdfs : FileSystem, name : Path) : Boolean = {
    hdfs.isDirectory(name)
  }
  def isFile(hdfs : FileSystem, name : String) : Boolean = {
    hdfs.isFile(new Path(name))
  }
  def isFile(hdfs : FileSystem, name : Path) : Boolean = {
    hdfs.isFile(name)
  }
  def createFile(hdfs : FileSystem, name : String) : Boolean = {
    hdfs.createNewFile(new Path(name))
  }
  def createFile(hdfs : FileSystem, name : Path) : Boolean = {
    hdfs.createNewFile(name)
  }
  def createFolder(hdfs : FileSystem, name : String) : Boolean = {
    hdfs.mkdirs(new Path(name))
  }
  def createFolder(hdfs : FileSystem, name : Path) : Boolean = {
    hdfs.mkdirs(name)
  }
  def exists(hdfs : FileSystem, name : String) : Boolean = {
    hdfs.exists(new Path(name))
  }
  def exists(hdfs : FileSystem, name : Path) : Boolean = {
    hdfs.exists(name)
  }
  def transport(inputStream : InputStream, outputStream : OutputStream): Unit ={
    val buffer = new Array[Byte](64 * 1000)
    var len = inputStream.read(buffer)
    while (len != -1) {
      outputStream.write(buffer, 0, len - 1)
      len = inputStream.read(buffer)
    }
    outputStream.flush()
    inputStream.close()
    outputStream.close()
  }
  class MyPathFilter extends PathFilter {
    override def accept(path: Path): Boolean = true
  }


  /**
    * get all file children's full name of a hdfs dir, not include dir children
    * @param fullName the hdfs dir's full name
    */
  def listChildren(hdfs : FileSystem, fullName : String, holder : ListBuffer[String]) : ListBuffer[String] = {
    val filesStatus = hdfs.listStatus(new Path(fullName), new MyPathFilter)
    for(status <- filesStatus){
      val filePath : Path = status.getPath
      if(isFile(hdfs,filePath))
        holder += filePath.toString
      else
        listChildren(hdfs, filePath.toString, holder)
    }
    holder
  }

  def copyFile(hdfs : FileSystem, source: String, target: String): Unit = {

    val sourcePath = new Path(source)
    val targetPath = new Path(target)

    if(!exists(hdfs, targetPath))
      createFile(hdfs, targetPath)

    val inputStream : FSDataInputStream = hdfs.open(sourcePath)
    val outputStream : FSDataOutputStream = hdfs.create(targetPath)
    transport(inputStream, outputStream)
  }

  //拷贝目录下的文件到目标文件
  def copyFiles(hdfs : FileSystem, sourceFolder: String ): Unit = {
    val holder: ListBuffer[String] = new ListBuffer[String]
    val children: List[String] = listChildren(hdfs, sourceFolder, holder).toList
    for (child <- children if !child.contains("_SUCCESS")) {
    val targetFolder = getNewPath(child)
      println(child)
      println(targetFolder)
     copyFile(hdfs, child,targetFolder )
     }
  }

  //文件名及路径转换
   def getNewPath(oldFilePath:String):String ={
     var str=""
      if(!new Path(oldFilePath).getName.contains("_SUCCESS") ){
        ///spark/emp/temp/201711112025/d=20171111/h=20/part-r-00000-6ba69620-ba52-4cb1-9ea0-6634ae0e16bc.txt
        ///spark/emp/data/d=171111/h=20/17111125-01.txt
          val oldpath =oldFilePath.split("/")
          str  =oldpath(0)+"//"+oldpath(2)+"/"+oldpath(3)+"/"+oldpath(4)+"/"+"data"+"/"+oldpath(7).substring(0,2)+oldpath(7).substring(4)+"/"+oldpath(8)+"/"+oldpath(6).substring(2,8)+oldpath(6).substring(10)+"-"+"0"+count+".txt"
          count+=1
      }  else{
          str=""
      }
     str
  }










}

