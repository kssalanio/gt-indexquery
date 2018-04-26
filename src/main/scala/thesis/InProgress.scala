package thesis

import java.io._
import java.nio.charset.Charset

import com.google.uzaygezen.core.{BitVector, BitVectorFactories, CompactHilbertCurve, MultiDimensionalSpec}
import geotrellis.proj4.{CRS, _}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff.{GeoTiff, MultibandGeoTiff}
import geotrellis.raster.resample.{Bilinear, NearestNeighbor}
import geotrellis.raster.{MultibandTile, TileLayout, _}
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index.hilbert._
import geotrellis.spark.tiling._
import geotrellis.util._
import geotrellis.vector._
import geotrellis.vector.io._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.shapefile.ShapefileDumper
import org.geotools.feature.DefaultFeatureCollection
import org.opengis.feature.simple.SimpleFeature
import thesis.Constants.{RDD_PARTS, TILE_SIZE}
import thesis.Refactored.{tile_ceiling_count, _}
import thesis.ShapeFileReader.readMultiPolygonFeatures

import scala.collection.JavaConverters._


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

  def repeat(len: Int, c: Char) = c.toString * len

  def padLeft(s: String, len: Int, c: Char) = {
    repeat(len - s.size, c) + s
  }

  def readGeotiffAndTile(file_path: String, output_dir_path: String)(implicit sc: SparkContext): Unit = {
    // Read GeoTiff file into Raster RDD
    val inputRdd: RDD[(ProjectedExtent, MultibandTile)] =
      sc.hadoopMultibandGeoTiffRDD(file_path)

    // Tiling layout to TILE_SIZE x TILE_SIZE grids
    val (_, rasterMetaData) =
      TileLayerMetadata.fromRdd(inputRdd, FloatingLayoutScheme(TILE_SIZE))

    val tiled_rdd: RDD[(SpatialKey, MultibandTile)] =
      inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
        .repartition(RDD_PARTS)

//    val negative_keys = tiled_rdd.filter(x => (x._1._1 < 0) || (x._1._2 < 0) )
//    println(">>> FOUND NEGATIVE KEYS")
//    pprint.pprintln(negative_keys)

    val resampleMethod = NearestNeighbor
    val mapTransform = rasterMetaData.layout.mapTransform

    // Reproject to WebMercator
    // TODO: check projection first and reproject only when needed
//    val (zoom, reprojected_rdd): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
//    MultibandTileLayerRDD(tiled_rdd, rasterMetaData)
//      .reproject(WebMercator, FloatingLayoutScheme(TILE_SIZE), Bilinear)
//    val final_crs = WebMercator

    // Reproject to EPSG:32651
    // TODO: check projection first and reproject only when needed
    val (zoom, reprojected_rdd): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
    MultibandTileLayerRDD(tiled_rdd, rasterMetaData)
      .reproject(CRS.fromEpsgCode(32651), FloatingLayoutScheme(TILE_SIZE), Bilinear)
    val final_crs = CRS.fromEpsgCode(32651)

    //    val negative_keys2 = reprojected_rdd.filter(x => (x._1._1 < 0) || (x._1._2 < 0) )
//    println(">>> FOUND NEGATIVE KEYS 2")
//    pprint.pprintln(negative_keys2)

    // Map function : write each tile in RDD to file
    println(">>> Constructing hilbert index from"
      +reprojected_rdd.keys.min().toString
      +" to "
      +reprojected_rdd.keys.max().toString)

    /**
      * TODO: Fix Hilbert Index to a universal TileLayout and Bounding Box
      */
    val hilbert_index = {
      val max_int = (math.pow(2, 32) - 1).toInt
      //new HilbertSpatialKeyIndex(KeyBounds(SpatialKey(0, 0), SpatialKey(max, max)), 31, 31)

      new HilbertSpatialKeyIndex(
        KeyBounds(
          SpatialKey(0, 0),
          SpatialKey(max_int, max_int)),
        reprojected_rdd.keys.max()._1,
        reprojected_rdd.keys.max()._2)
    }

    val result = reprojected_rdd
      // Filter out negative keys first to prevent errors
      .filter(x => (x._1._1 >= 0) && (x._1._2 >= 0) )
      .map{ tup =>
      val (spatial_key:SpatialKey, raster_tile:MultibandTile) = tup
      val extent : Extent = spatial_key.extent(rasterMetaData.layout)
      println("Indexing Spatial Key: "+spatial_key)
      val hilbert_hex = hilbert_index.toIndex(spatial_key).toHexString
      val padded_hex = padLeft(hilbert_hex, Constants.PAD_LENGTH, '0')
      //val gtif_file_path : String = output_dir_path+"_"+hilbert_hex+".tif"
      val gtif_file_path : String = os_path_join(output_dir_path,padded_hex+"_"+spatial_key._1+"_"+spatial_key._2+".tif")


      Holder.log.debug(s"Cutting $SpatialKey of ${raster_tile.dimensions} cells covering $extent to [$gtif_file_path]")
      // Write tile to GeoTiff file
      GeoTiff(raster_tile, extent, final_crs).write(gtif_file_path)
      (spatial_key:SpatialKey, extent)
      //TODO: Create Vector Layer outline of tiles using extents (reduce step)
    }
    println("Tiled RDD Items: " + tiled_rdd.count())
    println("Result RDD Items: " + result.count())
    println("Hilbert Dimensions: " + reprojected_rdd.keys.max())
    pprint.pprintln(result)
  }

  def invertHilbertIndex(hex_string :String, xResolution: Int, yResolution: Int): Array[BitVector] = {
    val chc = {
      val dimensionSpec =
        new MultiDimensionalSpec(
          List(
            xResolution,
            yResolution
          ).map(new java.lang.Integer(_)).asJava
        )

      new CompactHilbertCurve(dimensionSpec)
    }
    val bitVectors =
      Array(
        BitVectorFactories.OPTIMAL.apply(xResolution),
        BitVectorFactories.OPTIMAL.apply(yResolution)
      )
    val long_var = java.lang.Long.parseLong(hex_string, 16)
    val hilbertBitVector = BitVectorFactories.OPTIMAL.apply(chc.getSpec.sumBitsPerDimension)
    hilbertBitVector.copyFrom(long_var)
    chc.indexInverse(hilbertBitVector, bitVectors)
    return bitVectors
  }

  def readTiles(tile_dir_path: String, output_gtif_path: String)(implicit sc: SparkContext)={
    // Create Sequence of Geotiffs
    val tif_file_list = getListOfFiles(tile_dir_path,List[String]("tif"))
    val gtif_list : List[(String,MultibandGeoTiff)] = tif_file_list.map { tif_file =>
      (tif_file.getName,GeoTiffReader.readMultiband(tif_file.getAbsolutePath))//.raster.tile)
    }

    val tile_crs : geotrellis.proj4.CRS = gtif_list(0)._2.crs

    /**
      *TODO: use computed values instead fo dummy values
      * Maybe compute first and last tiles from filenames sorted by hex code?
     */

    val xResolution = 2
    val yResolution = 5

    val hilbert_index = {
      import geotrellis.spark.io.index.hilbert._
      //val max = (math.pow(2, 32) - 1).toInt
      //new HilbertSpatialKeyIndex(KeyBounds(SpatialKey(0, 0), SpatialKey(max, max)), 31, 31)

      new HilbertSpatialKeyIndex(
        KeyBounds(GridBounds(0,0,xResolution,yResolution)),
        xResolution,yResolution)
    }

    val x_vals = scala.collection.mutable.ArrayBuffer.empty[Double]
    val y_vals = scala.collection.mutable.ArrayBuffer.empty[Double]

    val mtl_seq : Seq[(SpatialKey,MultibandTile)] = gtif_list.map {
      list_item =>
        val filename : String  = list_item._1
        val hex_hilbert = filename.split("_")(0)
        val mband_gtif: MultibandGeoTiff = list_item._2
        //TODO:compute actual spatial key
        val bitvectors = invertHilbertIndex(hex_hilbert,xResolution,yResolution)

        pprint.pprintln(filename + " | "
          + bitvectors(0).toLong + " , "+ bitvectors(1).toLong
          + " | " + mband_gtif.extent)

        val spatialKey = SpatialKey(bitvectors(0).toLong.toInt,bitvectors(1).toLong.toInt)
        val mband_tile = mband_gtif.raster.tile

        if( spatialKey.equals(hilbert_index.keyBounds.minKey)){
          x_vals += mband_gtif.extent.xmin
          y_vals += mband_gtif.extent.ymax
        }

        if( spatialKey.equals(hilbert_index.keyBounds.maxKey)){
          x_vals += mband_gtif.extent.xmax
          y_vals += mband_gtif.extent.ymin
        }

        (spatialKey,mband_tile)
    }

    val extents = new Extent(x_vals.min, y_vals.min, x_vals.max, y_vals.max)
    println("Extents: "+extents.toString())

    // Create MultibandLayerRDD[K,V] with Metadata from RDD
    val mtl_rdd : RDD[(SpatialKey,MultibandTile)] = sc.parallelize(mtl_seq)

    //TODO: create combined metadata from each tile
//    val combined_raster_metadata = mtl_rdd.collectMetadata[SpatialKey](tile_crs, FloatingLayoutScheme(TILE_SIZE))
//
//    pprint.pprintln("RASTER METADATA: "+combined_raster_metadata)

//    val raster: Raster[MultibandTile] = MultibandTileLayerRDD(mtl_rdd, raster_metadata).stitch()
    val raster_tile: MultibandTile = mtl_rdd.stitch()

    // Compute Extent

    GeoTiff(raster_tile, extents, tile_crs)
      .write(output_gtif_path)
  }

  def readTiles_v2(tile_dir_path: String, output_gtif_path: String)(implicit sc: SparkContext)={
    // Create Sequence of Geotiffs
    val tif_file_list = getListOfFiles(tile_dir_path,List[String]("tif"))
    val test_rdd_tiles: RDD[(ProjectedExtent, MultibandTile)] = sc.hadoopMultibandGeoTiffRDD(tile_dir_path)
    println("Num Tiles: "+test_rdd_tiles.count())
  }

}
