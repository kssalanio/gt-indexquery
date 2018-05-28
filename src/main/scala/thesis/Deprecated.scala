package thesis

import java.io._
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
import geotrellis.vector.{Extent, Feature, Geometry, MultiPolygon, MultiPolygonFeature, ProjectedExtent}
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import thesis.Refactored.createGeoTiffMetadataJSON_withDSUid
import thesis.ThesisUtils.{getListOfFiles, os_path_join}

import scala.reflect.ClassTag

object Deprecated {
  def createInvertedIndex_old(tile_dir_path: String, run_rep: Int)(implicit spark_s : SparkSession)={
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

  /**
    * Create Metadata using Very Slow Cartesian Join
    *
    * @param dataset_uid
    * @param tile_dir_path
    * @param metadata_shp_filepath
    * @param spark_s
    * @return
    */
  def createTileMetadata_2(dataset_uid :String, tile_dir_path: String, metadata_shp_filepath :String)(implicit spark_s : SparkSession) = {
    val json_dir : File = new File(os_path_join(tile_dir_path,"json"))
    if (!json_dir.exists) json_dir.mkdir


    val metadata_fts : Seq[MultiPolygonFeature[Map[String,Object]]] = ShapeFileReader.readMultiPolygonFeatures(metadata_shp_filepath)
    val metadata_fts_rdd = spark_s.sparkContext.parallelize(metadata_fts, RDD_PARTS)

    val gtiff_files = spark_s.sparkContext.parallelize(getListOfFiles(tile_dir_path,List[String]("tif")), RDD_PARTS)

    val gtif_name_extent_rdd : RDD[(String, String, Extent)] = gtiff_files.map {
      tif_file =>

        // Write tile_file_name, tile_hex_code and tile_dataset_uid first
        val tif_file_name = tif_file.getName
        val tif_file_path = tif_file.getPath
        val hex_code = tif_file_name.split("_")(0)
        val tile_code_map = Map[String, Object]("tile_file_name" -> tif_file_name,"tile_hex_code" -> hex_code, "tile_dataset_uid" -> dataset_uid)
        new PrintWriter(
          os_path_join(json_dir.getAbsolutePath, tif_file.getName.replaceAll("\\.[^.]*$", "") + ".json"))
        {
          write(Json(DefaultFormats).write(tile_code_map) + "\n"); close }
        (tif_file_name, tif_file_path, GeoTiffReader.readMultiband(tif_file.getPath).extent)
    }

    val joined_rdd = metadata_fts_rdd.cartesian(gtif_name_extent_rdd).filter{
      case (metadata_ft,gtif_name_path_extent) =>
        metadata_ft.geom.intersects(gtif_name_path_extent._3) }.map{
      case (metadata_ft,gtif_name_path_extent) =>
        (gtif_name_path_extent._1, metadata_ft.data)
    }.groupByKey().collect().foreach {
      case (tif_file_name, meta_maps) =>
        val metadata_linestring = Json(DefaultFormats).write(meta_maps).filterNot(c => c  == '[' || c == ']').replaceAll("\\},\\{", "\\}\n\\{")
        val json_file_path = os_path_join(json_dir.getAbsolutePath, tif_file_name.replaceAll("\\.[^.]*$", "") + ".json")
        val json_writer = new FileWriter(json_file_path, true)
        json_writer.write(metadata_linestring+"\n")
        json_writer.close()
    }
  }

  /**
    * Create Metadata (faster version)
    *
    * @param dataset_uid
    * @param tile_dir_path
    * @param metadata_shp_filepath
    * @param spark_s
    * @return
    */
  def createTileMetadata_3(dataset_uid :String, tile_dir_path: String, metadata_shp_filepath :String)(implicit spark_s : SparkSession) = {
    val metadata_fts : Seq[MultiPolygonFeature[Map[String,Object]]] = ShapeFileReader.readMultiPolygonFeatures(metadata_shp_filepath)
    val metadata_fts_rdd : RDD[MultiPolygonFeature[Map[String,Object]]]= spark_s.sparkContext.parallelize(metadata_fts, RDD_PARTS)

    println("sizeEstimate - Metadata SHP: "+SizeEstimator.estimate(metadata_fts_rdd).toString)

    val gtiff_files = getListOfFiles(tile_dir_path,List[String]("tif"))
    val mband_gtiffs : List[(File, MultibandGeoTiff)] = gtiff_files.map { tif_file =>
      (tif_file,GeoTiffReader.readMultiband(tif_file.getAbsolutePath))
    }

    val merged_jsons = mband_gtiffs.map{
      list_item =>
        val (tif_file, gtiff) = list_item

        /**
          * //TODO: Optimize and prevent inefficient cartesion join
          */
        val merged_map_json : String = createGeoTiffMetadataJSON_withDSUid(dataset_uid, tif_file.getName, gtiff, metadata_fts_rdd)

        val result_rdd : RDD[MultiPolygonFeature[Map[String,Object]]] = metadata_fts_rdd.filter(
          ft => ft.geom.intersects(gtiff.extent)
        )

        val json_dir : File = new File(os_path_join(tile_dir_path,"json"))
        if (!json_dir.exists) json_dir.mkdir

        new PrintWriter(
          //          tif_file.getAbsoluteFile.toString.replaceAll("\\.[^.]*$", "") + ".json")
          os_path_join(json_dir.getAbsolutePath, tif_file.getName.replaceAll("\\.[^.]*$", "") + ".json"))
        {
          write(merged_map_json); close }
        merged_map_json
    }
  }

}
