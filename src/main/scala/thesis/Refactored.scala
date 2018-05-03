package thesis

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import thesis.Constants._
import geotrellis.proj4.CRS
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.kryo._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import geotrellis.spark.tiling._
import geotrellis.vector.{Feature, Geometry, MultiPolygon, MultiPolygonFeature, ProjectedExtent}
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.util._
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import thesis.InProgress.invertHilbertIndex

import scala.reflect.ClassTag

object Refactored {

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

  def readGeoTiffToMultibandTileLayerRDD(raster_filepath: String)
                (implicit sc: SparkContext) :
  MultibandTileLayerRDD[SpatialKey] = {

    // Read GeoTiff file into Raster RDD
    println(">>> Reading GeoTiff: "+raster_filepath)
    val input_rdd: RDD[(ProjectedExtent, MultibandTile)] =
      sc.hadoopMultibandGeoTiffRDD(raster_filepath)

    println(">>> Tiling GeoTiff")
    // Tiling layout to TILE_SIZE x TILE_SIZE grids
    val (_, raster_metadata) =
      TileLayerMetadata.fromRdd(input_rdd, FloatingLayoutScheme(TILE_SIZE))
    val tiled_rdd: RDD[(SpatialKey, MultibandTile)] =
      input_rdd
        .tileToLayout(raster_metadata.cellType, raster_metadata.layout, Bilinear)
    return MultibandTileLayerRDD(tiled_rdd, raster_metadata)
  }

  def maskRaster(mtl_rdd: MultibandTileLayerRDD[SpatialKey], shp_filepath: String)
                (implicit sc: SparkContext) :
  RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] ={

    // Read shapefile into MultiPolygon
    println(">>> Reading shapefile: "+shp_filepath)
    val features = ShapeFileReader.readMultiPolygonFeatures(shp_filepath)
    val region: MultiPolygon = features(0).geom
    val attribute_table = features(0).data

    // Mask and return resulting raster
    return mtl_rdd.mask(region)
  }

  def createIndexedMultibandTileRDD(mbt_RDD: RDD[(ProjectedExtent, MultibandTile)], rasterMetaData: TileLayerMetadata[SpatialKey]):
  MultibandTileLayerRDD[SpatialKey] =
  {
    val tiled_rdd: RDD[(SpatialKey, MultibandTile)] =
      mbt_RDD
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
    val mtl_rdd = MultibandTileLayerRDD(tiled_rdd, rasterMetaData)
    return mtl_rdd
  }

  def filterMetadataShapefile(metadata_shp_filepath: String,
                              qry_shp_filepath: String,
                              layout: LayoutDefinition,
                              crs: CRS)
                             (implicit sc: SparkContext):
  RDD[(SpatialKey, Feature[Geometry,Map[String,Object]])] =
  {
    val metadata_fts : Seq[MultiPolygonFeature[Map[String,Object]]] = ShapeFileReader.readMultiPolygonFeatures(metadata_shp_filepath)
    val metadata_fts_rdd : RDD[MultiPolygonFeature[Map[String,Object]]]= sc.parallelize(metadata_fts, RDD_PARTS)

    // Test Intersection
    val query_ft = ShapeFileReader.readMultiPolygonFeatures(qry_shp_filepath)
//    val region: MultiPolygon = query_ft(0).geom
//    val attribute_table = query_ft(0).data

    // Filter shapefile metadata_feats to query geometry
    val result_rdd = metadata_fts_rdd.filter(
      ft => ft.geom.intersects(query_ft(0).geom)
    )
    println("Source RDD: "+metadata_fts_rdd.count())
    println("Filtered RDD: "+result_rdd.count())


    return metadata_fts_rdd.clipToGrid(layout)

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


  def createTileMetadata(tile_dir_path: String, metadata_shp_filepath :String)(implicit spark_s : SparkSession) = {
    val metadata_fts : Seq[MultiPolygonFeature[Map[String,Object]]] = ShapeFileReader.readMultiPolygonFeatures(metadata_shp_filepath)
    val metadata_fts_rdd : RDD[MultiPolygonFeature[Map[String,Object]]]= spark_s.sparkContext.parallelize(metadata_fts, RDD_PARTS)

    val gtiff_files = getListOfFiles(tile_dir_path,List[String]("tif"))
    val mband_gtiffs : List[(File, MultibandGeoTiff)] = gtiff_files.map { tif_file =>
      (tif_file,GeoTiffReader.readMultiband(tif_file.getAbsolutePath))
    }

    val merged_jsons = mband_gtiffs.map{
      list_item =>
        val (tif_file, gtiff) = list_item
        //TODO: Handle empty intersection (no features intersecting the tile)
        val merged_map_json : String = createGeoTiffMetadataJSON(tif_file.getName, gtiff, metadata_fts_rdd)
        val json_dir : File = new File(os_path_join(tile_dir_path,"json"))
        if (!json_dir.exists) json_dir.mkdir

        new PrintWriter(
//          tif_file.getAbsoluteFile.toString.replaceAll("\\.[^.]*$", "") + ".json")
          os_path_join(json_dir.getAbsolutePath, tif_file.getName.replaceAll("\\.[^.]*$", "") + ".json"))
        {
          write(merged_map_json); close }
        merged_map_json
    }
    println(merged_jsons)
    pprint.pprintln(merged_jsons)
  }

  def createGeoTiffMetadataJSON(tif_filename:String, tif_file: MultibandGeoTiff, metadata_fts_rdd: RDD[MultiPolygonFeature[Map[String, Object]]])(implicit spark_s : SparkSession): String = {
    val result_rdd : RDD[MultiPolygonFeature[Map[String,Object]]] = metadata_fts_rdd.filter(
      ft => ft.geom.intersects(tif_file.extent)
    )
    val ft_count =result_rdd.count()

    val hex_code = tif_filename.split("_")(0)
    val tile_code_map = Map[String, Object]("tile_file_name" -> tif_filename,"tile_hex_code" -> hex_code)

    if(ft_count >= 1) {
      Json(DefaultFormats).write(tile_code_map) + ",\n" +
        result_rdd.aggregate[String]("")(
          {(acc, cur_ft) =>   // Combining accumulator and element
            val map_json = Json(DefaultFormats).write(cur_ft.data)
            if(acc.length > 0 && map_json.length > 0){
              acc + ",\n" + map_json
            }else{
              acc + map_json
            }
          },
          {(acc1, acc2) =>  // Combining accumulator and another accumulator
            if(acc1.length > 0 && acc2.length > 0) {
              acc1 + ",\n" + acc2
            }else{
              acc1 + acc2
            }
          }
        )
    }else{
      Json(DefaultFormats).write(tile_code_map) + ",\n" +
        Json(DefaultFormats).write(Map[String, Object]())
    }
  }

  def createGeoTiffMetadataJSON_2(dataset_uid :String,tif_filename:String, tif_file: MultibandGeoTiff, metadata_fts_rdd: RDD[MultiPolygonFeature[Map[String, Object]]])(implicit spark_s : SparkSession): String = {
    val result_rdd : RDD[MultiPolygonFeature[Map[String,Object]]] = metadata_fts_rdd.filter(
      ft => ft.geom.intersects(tif_file.extent)
    )
    val ft_count =result_rdd.count()

    val hex_code = tif_filename.split("_")(0)
    val tile_code_map = Map[String, Object]("tile_file_name" -> tif_filename,"tile_code" -> hex_code, "tile_dataset_uid" -> dataset_uid)

    if(ft_count >= 1) {
      Json(DefaultFormats).write(tile_code_map) + ",\n" +
        result_rdd.aggregate[String]("")(
          {(acc, cur_ft) =>   // Combining accumulator and element
            val map_json = Json(DefaultFormats).write(cur_ft.data)
            if(acc.length > 0 && map_json.length > 0){
              acc + ",\n" + map_json
            }else{
              acc + map_json
            }
          },
          {(acc1, acc2) =>  // Combining accumulator and another accumulator
            if(acc1.length > 0 && acc2.length > 0) {
              acc1 + ",\n" + acc2
            }else{
              acc1 + acc2
            }
          }
        )
    }else{
      Json(DefaultFormats).write(tile_code_map) + ",\n" +
        Json(DefaultFormats).write(Map[String, Object]())
    }
  }

//  def jsonStrToMap(jsonStr: String): Map[String, Any] = {
//    Json(DefaultFormats).parse(jsonStr).extract[Map[String, Any]]
//  }

  def createInvertedIndex(tile_dir_path: String, run_rep: Int)(implicit spark_s : SparkSession)={
    //val json_files = getListOfFiles(tile_dir_path,List[String]("json"))
    val json_files_rdd = spark_s.sparkContext.wholeTextFiles(tile_dir_path)
    json_files_rdd.flatMap {
      case (path, text) =>
        text.trim.filterNot(c => c  == '{' || c == '}').split("\",\"").map{
                    text_string =>
                      text_string.filterNot(c => c  == '"').split(":")(0)
                  }.map(keyword => (keyword, path))
    }
    .map {
      case (word, path) => ((word, path), 1)
    }
    .reduceByKey{    // Count the equal (word, path) pairs, as before
      (count1, count2) => count1 + count2
    }
    .map {           // Rearrange the tuples; word is now the key we want.
      case ((word, path), n) => (word, (path, n))
    }
    .groupByKey
    .mapValues(iterator => iterator.mkString(", "))
    .saveAsTextFile(tile_dir_path+"/inverted_idx_"+run_rep)
  }

  def createInvertedIndex_2(tile_dir_path: String, run_rep: Int)(implicit spark_s : SparkSession)={
    //val json_files = getListOfFiles(tile_dir_path,List[String]("json"))
    val json_files_rdd = spark_s.sparkContext.wholeTextFiles(tile_dir_path)
    json_files_rdd.flatMap {
      case (path, text) => {
        val (first_line, other_lines) = text.trim.split("\n").splitAt(1)
        val fl_tokens = first_line(0).filterNot(c => c  == '{' || c == '}' | c == '"').split(",").map {
          text_string =>
            text_string.filterNot(c => c == '"').split(":")
        }
//        println("FL_TOKENS: ")
//        pprint.pprintln(fl_tokens)

        val tile_code = fl_tokens(1)(1)
        val tile_dataset_uid = fl_tokens(2)(1)
        other_lines.flatMap{
          text_line =>
            text_line.trim.filterNot(c => c  == '{' || c == '}').split("\",\"").map{
              text_string =>
                text_string.filterNot(c => c  == '"').split(":")(0)
            }.map(keyword => (keyword, keyword, path, tile_code, tile_dataset_uid))

        }
      }
    }.map {
        case (keyword, keyword_path, path, tile_code, tile_dataset_uid) => (keyword, (keyword_path, path, tile_code, tile_dataset_uid))
    }.groupByKey
    .mapValues(iterator => iterator.mkString(", "))
    .saveAsTextFile(tile_dir_path+"/inverted_idx_"+run_rep)
  }

}
