package demo

import java.io.File

import demo.Constants._

object Refactored {

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
}
