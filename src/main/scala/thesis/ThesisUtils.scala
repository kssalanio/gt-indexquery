package thesis

import java.io.{BufferedWriter, File, FileWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import thesis.Constants.TILE_SIZE

import scala.reflect.ClassTag

object ThesisUtils {
  /**
    * Tiling methods wrt to @TILE_SIZE
    * @param x - value needed to be tiled
    * @return - value in tile length
    */
  def tile_floor_count(x: Double): Int = {
    return Math.floor(x/TILE_SIZE.toDouble).toInt
  }

  def tile_floor_val(x: Double): Int = {
    return tile_floor_count(x) * TILE_SIZE
  }

  def tile_ceiling_count(x: Double): Int = {
    return Math.ceil(x/TILE_SIZE.toDouble).toInt
  }

  def tile_ceiling_val(x: Double): Int = {
    return tile_ceiling_count(x) * TILE_SIZE
  }

  def time[R](f: => R): (R, Long) = {
    val t1 = System.nanoTime
    val ret = f
    val t2 = System.nanoTime
    (ret, t2 - t1)
  }

  def getListOfFiles(dir_path: String):List[File] = {
    val dir = new File(dir_path)
    dir.listFiles.filter(_.isFile).toList
  }

  def getListOfFiles(dir_path: String, extensions: List[String]): List[File] = {
    val dir = new File(dir_path)
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }


  def getClassTag[T](v: T)(implicit ev: ClassTag[T]) = ev.toString

  def unsafeAddDir(dir: String) = try {
    val field = classOf[ClassLoader].getDeclaredField("usr_paths")
    field.setAccessible(true)
    val paths = field.get(null).asInstanceOf[Array[String]]
    if(!(paths contains dir)) {
      field.set(null, paths :+ dir)
      System.setProperty("java.library.path",
        System.getProperty("java.library.path") +
          java.io.File.pathSeparator +
          dir)
    }
  } catch {
    case _: IllegalAccessException =>
      error("Insufficient permissions; can't modify private variables.")
    case _: NoSuchFieldException =>
      error("JVM implementation incompatible with path hack")
  }

  /**
    * Writes a given @list to a file @filepath, each element is written per line
    * @param list
    * @param filepath
    */
  def writeListToFile(list: Seq[Any], filepath: String) : Unit = {
    val writer = new BufferedWriter(new FileWriter(filepath))
    List("this\n","that\n","other\n").foreach(writer.write)
    writer.close()
  }

  def getCurrentDateAndTime(format_str : String) : String = {
    return (new SimpleDateFormat(format_str)).format(Calendar.getInstance().getTime())
  }

  def roundAt(p: Int)(n: Double): Double = { val s = math pow (10, p); (math round n * s) / s }

  def nanosecToSec(nanosecs: Long): Double = {
    nanosecs.asInstanceOf[Double]
    return roundAt(4)(nanosecs.asInstanceOf[Double]/1000000000.0)
  }

  def getFileSize(filepath:String, pow: Double): Double ={
    return roundAt(3)((new File(filepath)).length.asInstanceOf[Double]/Math.pow(1024.0,pow))
  }

  def countPointsSHP(shp_filepath:String): Long = {
    val features = ShapeFileReader.readMultiPolygonFeatures(shp_filepath)
    return features.map(ft =>{
      ft.geom.vertexCount
    }).foldLeft(0)(_ + _)
  }
  def countFeaturesSHP(shp_filepath:String): Int = {
    return ShapeFileReader.readMultiPolygonFeatures(shp_filepath).length
  }

  def os_path_join(path1: String, path2: String): String = {
    new File(path1,path2).getPath
  }

  def repeat(len: Int, c: Char) = c.toString * len

  def padLeft(s: String, len: Int, c: Char) = {
    repeat(len - s.size, c) + s
  }

}
