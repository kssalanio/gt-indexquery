package thesis

import java.io._
import java.nio.file._
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

import thesis.Constants.{RDD_PARTS, TILE_SIZE}
import thesis.Main.output_path
import thesis.Refactored.tile_ceiling_count
import thesis.ShapeFileReader.readMultiPolygonFeatures
import geotrellis.proj4.{CRS, _}
import geotrellis.raster.io.geotiff.{GeoTiff, MultibandGeoTiff}
import geotrellis.raster.resample.{Bilinear, NearestNeighbor}
import geotrellis.raster.{MultibandTile, TileLayout, _}
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.kryo.KryoRegistrator
import geotrellis.spark.tiling._
import geotrellis.util._
import geotrellis.vector._
import geotrellis.vector.io._
import org.apache.spark.{SparkContext, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.geotools.data.shapefile.ShapefileDumper
import org.geotools.feature.DefaultFeatureCollection
import org.opengis.feature.simple.SimpleFeature
import scala.reflect.ClassTag
import collection.JavaConverters._


object InProgress {
  /**
    * TODO: Combine to one GEOJSON
    * TODO: Validate GEOJSON
    *
    * @param input_rdd
    * @param crs
    * @param output_dir
    */
  def writeToGeoJSON(input_rdd:  RDD[MultiPolygonFeature[Map[String,Object]]], crs: CRS, output_dir : String) : Unit = {
    input_rdd.map(ft => {
      val ft_name = ft.data("Block_Name").toString()
      println("Writing: "+ft_name)
      Some(new PrintWriter(new File(output_dir+ft_name+"_geom.geojson")) { write(ft.geom.toGeoJson.toString); close })
      Some(new PrintWriter(new File(output_dir+ft_name+"_data.json")) { write(scala.util.parsing.json.JSONObject(ft.data).toString()); close })
    }).count()
  }

  /**
    * Errors out, still needs to be debugged
    *
    * @param input_rdd
    * @param crs
    */
  def writeToESRIShp(input_rdd:  RDD[MultiPolygonFeature[Map[String,Object]]], crs: CRS) : Unit = {


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

  /**
    * Multifeature .shp intersected with another multifeature .shp
    * TODO: filter query with intersect?
    * @param library_shp_filepath
    * @param query_shp_filepath
    */
  def readShapefilesAndIntersect(library_shp_filepath:String, query_shp_filepath:String) : Unit = {
    val lib_features : Seq[MultiPolygonFeature[Map[String,Object]]] = readMultiPolygonFeatures(library_shp_filepath)
    val qry_features : Seq[MultiPolygonFeature[Map[String,Object]]] = readMultiPolygonFeatures(query_shp_filepath)

    //    val groupedPolys: RDD[(SpatialKey, Iterable[MultiPolygonFeature[UUID]])] =
    //      lib_features.

    // Convert lib_features into vector RDD
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



}
