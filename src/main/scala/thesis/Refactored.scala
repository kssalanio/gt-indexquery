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
import geotrellis.util._

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
}
