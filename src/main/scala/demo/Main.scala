package demo

import java.io._

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.mask._
import geotrellis.spark.io._
import geotrellis.spark.io.file._
import geotrellis.spark.io.file.FileLayerReader
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.kryo._
import geotrellis.spark.tiling._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.tags.EPSGProjectionTypes
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.{MultiPolygonFeature, _}
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.serializer.KryoSerializer
import geotrellis.util._
import org.geotools.feature.{DefaultFeatureCollection, FeatureCollection}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable
import scala.io.StdIn
import scala.reflect.ClassTag
import collection.JavaConverters._

//import geotrellis.gdal._
import org.gdal._
import org.gdal.gdal.Dataset
import org.gdal.gdalconst.gdalconstConstants
import ShapeFileReader._
import com.vividsolutions.jts
//import sext._

import demo.Constants._
import demo.Refactored._


object Main{
  //val logger = Logger.getLogger(Main.getClass)

  def bootSentence = "\n\n>>> INITIALIZE <<<\n\n"
  def helloSentence  = "Hello GeoTrellis"

  val test_gtif: String = "/home/spark/datasets/SAR/geonode_sar_guimaras.tif"
  val catalog_dirpath: String = "/home/spark/datasets/SAR"
  var output_path : String = "/home/spark/datasets/output/tiled"
  //val test_gtif: String = "/shared/DATA/datasets/SAR/geonode_sar_guimaras.tif"
  //val catalog_dirpath: String = "/shared/DATA/datasets/SAR"

  def log(msg: String){
    println("> "+msg)
  }

  def createSparkContext(): Unit = {
    val conf = new org.apache.spark.SparkConf()
    conf.setMaster("local[*]")
    implicit val sc = geotrellis.spark.util.SparkUtils.createSparkContext("Test console", conf)
  }


  /**
    * Create Sequence of Spatial Keys from a tile's extents
    *
    * @param tile_extents
    * @tparam D
    * @return
    */
  def createSpatialKey[D](tile_extents: Extent, tile_layout: TileLayout, layout_def: LayoutDefinition): Seq[SpatialKey] = {
    val gridBounds = layout_def.mapTransform(tile_extents)

    for {
      (c, r) <- gridBounds.coords
      if r < tile_layout.totalRows
      if c < tile_layout.totalCols
    } yield SpatialKey(c, r)
  }

  def createSpatialKeyFromTiff(multiband_gtiff : MultibandGeoTiff): Seq[SpatialKey] = {
    val col_count = tile_ceiling_count(multiband_gtiff.extent.width)
    val row_count = tile_ceiling_count(multiband_gtiff.extent.height)
    val tile_layout = TileLayout(col_count, row_count, TILE_SIZE, TILE_SIZE)
    val layout_def = LayoutDefinition(multiband_gtiff.extent, tile_layout)

    //TODO: replace with Quad Tree indexed spatial keys
    return createSpatialKey(multiband_gtiff.extent, tile_layout, layout_def)
  }

  def readTiles(file_path: String): Unit = {

    // Spark config
    val conf =
      new SparkConf()
        .setMaster("local")
        .setAppName("IndexQuery")
        //.set("spark.default.parallelism", "4")
        .set("spark.serializer", classOf[KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
    System.setProperty("spark.ui.enabled", "true")
    System.setProperty("spark.ui.port", "4040")

    // Init spark context
    val spark_context = new SparkContext(conf)

    // Read GeoTiff file into Raster RDD
    val inputRdd: RDD[(ProjectedExtent, MultibandTile)] =
      spark_context.hadoopMultibandGeoTiffRDD(file_path)

    // Tiling layout to TILE_SIZE x TILE_SIZE grids
    val (_, rasterMetaData) =
      TileLayerMetadata.fromRdd(inputRdd, FloatingLayoutScheme(TILE_SIZE))

    val tiled_rdd: RDD[(SpatialKey, MultibandTile)] =
      inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
        .repartition(RDD_PARTS)

    val resampleMethod = NearestNeighbor
    val mapTransform = rasterMetaData.layout.mapTransform

    // Reproject to WebMercator
    // TODO: check projection first and reproject only when needed
    val (zoom, reprojected_rdd): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
          MultibandTileLayerRDD(tiled_rdd, rasterMetaData)
            .reproject(WebMercator, FloatingLayoutScheme(TILE_SIZE), Bilinear)
    val final_crs = WebMercator

    // Map function : write each tile in RDD to file
    val result = reprojected_rdd.map{ tup =>
      val (spatial_key:SpatialKey, raster_tile:MultibandTile) = tup
      val extent : Extent = spatial_key.extent(rasterMetaData.layout)
      val gtif_file_path : String = output_path+"/guimaras_"+spatial_key.col+"_"+spatial_key.row+".tif"
      Holder.log.debug(s"Cutting $SpatialKey of ${raster_tile.dimensions} cells covering $extent to [$gtif_file_path]")
      // Write tile to GeoTiff file
      GeoTiff(raster_tile, extent, final_crs).write(gtif_file_path)
      (spatial_key:SpatialKey, extent)
      //TODO: Create Vector Layer outline of tiles using extents (reduce step)
    }
    println("Tiled RDD Items: " +tiled_rdd.count())
    println("Result RDD Items: " +result.count())
    pprint.pprintln(result)
    System.in.read();
    //spark_context.stop(); //
  }

  def gdal_raw_cutline_test(input_raster: String, output_raster: String, input_shp: String): Unit = {
    val vector_warp_opts = new java.util.Vector[java.lang.String]()
    vector_warp_opts.add("-crop_to_cutline")
    vector_warp_opts.add("-cutline")
    vector_warp_opts.add(input_shp)

    val src_ds : Array[Dataset] = Array(gdal.gdal.Open(input_raster, gdalconstConstants.GA_ReadOnly))
    //val src_ds = {gdal.gdal.Open(input_raster, gdalconstConstants.GA_ReadOnly)}
    //val dst_ds : Dataset = gdal.gdal.Open(output_raster, gdalconstConstants.GA_Update)
    val dst_ds = output_raster

    //Warp(Dataset dstDS, Dataset[] object_list_count, WarpOptions warpAppOptions)
    val cutline_warp_opts = new gdal.WarpOptions(vector_warp_opts)
    val out_ds = org.gdal.gdal.gdal.Warp(dst_ds, src_ds, cutline_warp_opts)
  }

  def gdal_create_empty_raster(filepath : String): Unit = {
    val NROWS = 1
    val NCOLS = 1

    val driver: org.gdal.gdal.Driver = org.gdal.gdal.gdal.GetDriverByName("GTiff")
    //val dataset = driver.Create(filepath, NROWS, NCOLS, 1, gdalconstConstants.GDT_Float32)
    return driver.Create(filepath, NROWS, NCOLS, 1, gdalconstConstants.GDT_Float32)
  }

  /**
    * Convert from Raster RDD to GDAL dataset
    */
  def gdal_rasterrdd_to_gdalds(): Unit = {
    //TODO: convert from Raster RDD to GDAL dataset and back
  }

  def gdal_clip(filepath : String): Unit = {
    //TODO: clip raster tiles using full vector shp
    //TODO: stitch together clipped tiles

  }

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

  def readShapefilesAndIntersect(library_shp_filepath:String, query_shp_filepath:String) : Unit = {
    val lib_features : Seq[MultiPolygonFeature[Map[String,Object]]] = readMultiPolygonFeatures(library_shp_filepath)
    val qry_features : Seq[MultiPolygonFeature[Map[String,Object]]] = readMultiPolygonFeatures(query_shp_filepath)

//    val groupedPolys: RDD[(SpatialKey, Iterable[MultiPolygonFeature[UUID]])] =
//      lib_features.

    // Convert lib_features into vector RDD
  }

  def getClassTag[T](v: T)(implicit ev: ClassTag[T]) = ev.toString

  def readShapefileToRDD(filepath: String,
                         layout: LayoutDefinition,
                         crs: CRS,
                         output_dir: String)
                        (implicit sc: SparkContext):
  RDD[(SpatialKey, Feature[Geometry,Map[String,Object]])] =
  {
    val features : Seq[MultiPolygonFeature[Map[String,Object]]] = ShapeFileReader.readMultiPolygonFeatures(filepath)
    val feature_rdd : RDD[MultiPolygonFeature[Map[String,Object]]]= sc.parallelize(features, RDD_PARTS)
    println(getClassTag(feature_rdd))
    //val groupedPolys: RDD[(SpatialKey, Iterable[MultiPolygonFeature[UUID]])] =

    // Test Intersection
    val vector_filepath  = "/home/spark/datasets/vectors/reprojected/san_lorenzo.shp"
    val query_ft = ShapeFileReader.readMultiPolygonFeatures(vector_filepath)
    val region: MultiPolygon = features(0).geom
    val attribute_table = features(0).data


    // Filter shapefile features to query geometry
    val result_rdd = feature_rdd.filter(
      ft => ft.geom.intersects(query_ft(0).geom)
    )
    println("Source RDD: "+feature_rdd.count())
    println("Filtered RDD: "+result_rdd.count())

    //writeToESRIShp(result_rdd, crs)
    //writeToGeoJSON(result_rdd, crs, "/home/spark/datasets/output/filtered_geojson/")

    // Write Feature UIDs to file
//    val ft_uids = result_rdd.map(ft =>
//        ft.data("UID")
//      ).collect()
//    Some(new PrintWriter(new File(output_dir+"feature_uids.list")) { write(ft_uids.mkString("\n")); close })

    // Write filtered results to file

    return feature_rdd.clipToGrid(layout)

  }

  def writeListToFile(list: Seq[Any], filepath: String) : Unit = {
    val writer = new BufferedWriter(new FileWriter(filepath))
    List("this\n","that\n","other\n").foreach(writer.write)
    writer.close()
  }

  def writeToGeoJSON(input_rdd:  RDD[MultiPolygonFeature[Map[String,Object]]], crs: CRS, output_dir : String) : Unit = {
    input_rdd.map(ft => {
      val ft_name = ft.data("Block_Name").toString()
      println("Writing: "+ft_name)
      Some(new PrintWriter(new File(output_dir+ft_name+"_geom.geojson")) { write(ft.geom.toGeoJson.toString); close })
      Some(new PrintWriter(new File(output_dir+ft_name+"_data.json")) { write(scala.util.parsing.json.JSONObject(ft.data).toString()); close })
    }).count()
  }

  def writeToESRIShp(input_rdd:  RDD[MultiPolygonFeature[Map[String,Object]]], crs: CRS) : Unit = {
    import org.geotools.data.shapefile.ShapefileDumper
    import org.geotools.data.simple.SimpleFeatureCollection
    import java.nio.charset.Charset

    val dumper = new ShapefileDumper(new File("/home/spark/datasets/output/filtered_geojson/filtered.shp"))
    // optional, set a target charset (ISO-8859-1 is the default)
    dumper.setCharset(Charset.forName("ISO-8859-15"))
    // split when shp or dbf reaches 100MB
    val maxSize = 100 * 1024 * 1024
    dumper.setMaxDbfSize(maxSize)
    dumper.setMaxDbfSize(maxSize)
    // actually dump data
    val fc : DefaultFeatureCollection = new DefaultFeatureCollection()
    val sq_sf : Seq[SimpleFeature]=
      input_rdd.flatMap { ft =>
        try {
          Some(GeometryToSimpleFeature(
            ft.geom,
            Some(crs),
            ft.data.toSeq))
        } catch {
          case e: Exception =>
            // Log error
            None
        }
      }.collect()

    fc.addAll(sq_sf.asJavaCollection)
    dumper.dump(fc)
  }

  def testMaskRaster(raster_filepath: String, shp_filepath: String, output_dir: String)(implicit sc: SparkContext) : Unit = {

    // Read GeoTiff file into Raster RDD
    println(">>> Reading GeoTiff")
    val inputRdd: RDD[(ProjectedExtent, MultibandTile)] =
      sc.hadoopMultibandGeoTiffRDD(raster_filepath)

    println(">>> Tiling GeoTiff")
    // Tiling layout to TILE_SIZE x TILE_SIZE grids
    val (_, rasterMetaData) =
      TileLayerMetadata.fromRdd(inputRdd, FloatingLayoutScheme(TILE_SIZE))

//    val tiled_rdd: RDD[(SpatialKey, MultibandTile)] =
//      inputRdd
//        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
//    val mtl_rdd = MultibandTileLayerRDD(tiled_rdd, rasterMetaData)

    readShapefileToRDD(
      "/home/spark/datasets/vectors/reprojected/lidar_coverage_2017_05/lidar_coverage.shp",
      rasterMetaData.layout,
      rasterMetaData.crs,
      output_dir)
  }

  def testMaskRaster2(
                       catalog_path: String,
                       raster_filepath: String,
                       raster_filename:String,
                       vector_filepath: String,
                       output_dir : String)
                     (implicit sc: SparkContext)
                      : Unit = {
    /**
      * http://geotrellis.readthedocs.io/en/latest/guide/spark.html?highlight=mask
      */


    // Read GeoTiff file into Raster RDD
    println(">>> Reading GeoTiff")
    val inputRdd: RDD[(ProjectedExtent, MultibandTile)] =
      sc.hadoopMultibandGeoTiffRDD(raster_filepath)

    println(">>> Tiling GeoTiff")
    // Tiling layout to TILE_SIZE x TILE_SIZE grids
    val (_, rasterMetaData) =
      TileLayerMetadata.fromRdd(inputRdd, FloatingLayoutScheme(TILE_SIZE))
    val tiled_rdd: RDD[(SpatialKey, MultibandTile)] =
      inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
    val mtl_rdd = MultibandTileLayerRDD(tiled_rdd, rasterMetaData)

    // Read into type 'MultiPolygon' from 'vector_filepath'
//    val features = ShapeFileReader.readSimpleFeatures(vector_filepath)
//    val region: MultiPolygon = features(0).geom[jts.MultiPolygon]
    val features = ShapeFileReader.readMultiPolygonFeatures(vector_filepath)
    val region: MultiPolygon = features(0).geom
    val attribute_table = features(0).data
    //val region: MultiPolygon = features(0).reproject(CRS.fromName("EPSG:4326"), mtl_rdd.metadata.crs)


    // Masking and Stitching results
    println(">>> Masking and Stitching")

    val extents = region.envelope
    val test_rdd = mtl_rdd.mask(region)

    println(">>> RDD count: "+test_rdd.count())

    val raster: Raster[MultibandTile] = test_rdd.stitch() //TODO: trim to extent/envelope of vector. Crop stretches it to its own extents
    println("Shapefile Extent: ")
    pprint.pprintln(region.envelope)

    println("Raster Extent: ")
    pprint.pprintln(raster.extent)

    // Write Result to GeoTiff file
    //GeoTiff(raster, queryResult.metadata.crs).write(output_dir+"test_result.tif")
    GeoTiff(raster, rasterMetaData.crs).write(output_dir+"test_result.tif")

  }


  def main(args: Array[String]): Unit = {
    //Initialize
    println(System.getProperty("java.library.path"))
    unsafeAddDir("/usr/lib/jni/")
    println(System.getProperty("java.library.path"))
    org.gdal.gdal.gdal.AllRegister()
    println(bootSentence)

    val conf =
      new SparkConf()
        .setMaster("local")
        .setAppName("IndexQuery")
        //.set("spark.default.parallelism", "4")
        .set("spark.serializer", classOf[KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
    System.setProperty("spark.ui.enabled", "true")
    System.setProperty("spark.ui.port", "4040")

    // Init spark context
    implicit val sc = new SparkContext(conf)

    try {
      run(sc)
      // Pause to wait to close the spark context,
      // so that you can check out the UI at http://localhost:4040
      println("Hit enter to exit.")
      StdIn.readLine()
    } finally {
      sc.stop()
    }
  }

  def run(implicit sc: SparkContext) = {
//    testMaskRaster2(catalog_path="/home/spark/datasets/SAR/test/",
//      raster_filepath="/home/spark/datasets/SAR/test/geonode_sar_guimaras.tif",
//      raster_filename="geonode_sar_guimaras",
//      vector_filepath="/home/spark/datasets/vectors/reprojected/san_lorenzo.shp",
//      output_dir ="/home/spark/datasets/output/mask/")

//    readShapefileToRDD(
//      "/shared/DATA/datasets/vectors/reprojected/lidar_coverage_2017_05/lidar_coverage.shp",
//      FloatingLayoutScheme(TILE_SIZE))

    testMaskRaster(
      "/home/spark/datasets/SAR/test/geonode_sar_guimaras.tif",
      "/home/spark/datasets/vectors/reprojected/san_lorenzo.shp",
      "/home/spark/datasets/output/filtered_geojson/")

    //TODO:
    /**
      *       1. Master Quad Tree Spatial Key
      *         (a) generate prefix tree
      *       2. Tile Quad Tree
      *         (a) Quad Tree down to the pixel? For clipping to Vector outline?
      *         (b) A faster way than method (a)
      */

    println("\n\n>> END <<\n\n")
  }
}

object Holder extends Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)
}