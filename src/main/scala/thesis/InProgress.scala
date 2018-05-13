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
import geotrellis.spark.io.Intersects
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index.KeyIndex
import geotrellis.spark.io.index.hilbert._
import geotrellis.spark.io.index.zcurve.{Z2, ZSpatialKeyIndex}
import geotrellis.spark.tiling._
import geotrellis.util._
import geotrellis.vector._
import geotrellis.vector.io._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator
import org.geotools.data.shapefile.ShapefileDumper
import org.geotools.feature.DefaultFeatureCollection
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
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
  def writeToGeoJSON(input_rdd: RDD[MultiPolygonFeature[Map[String, Object]]], crs: CRS, output_dir: String): Unit = {
    input_rdd.map(ft => {
      val ft_name = ft.data("Block_Name").toString()
      println("Writing: " + ft_name)
      Some(new PrintWriter(new File(output_dir + ft_name + "_geom.geojson")) {
        write(ft.geom.toGeoJson.toString); close
      })
      Some(new PrintWriter(new File(output_dir + ft_name + "_data.json")) {
        write(scala.util.parsing.json.JSONObject(ft.data).toString()); close
      })
    }).count()
  }

  /**
    * Errors out, still needs to be debugged
    *
    * @param input_rdd
    * @param crs
    */
  def writeToESRIShp(input_rdd: RDD[MultiPolygonFeature[Map[String, Object]]], crs: CRS): Unit = {


    val dumper = new ShapefileDumper(new File("/home/spark/datasets/output/filtered_geojson/filtered.shp"))
    // optional, set a target charset (ISO-8859-1 is the default)
    dumper.setCharset(Charset.forName("ISO-8859-15"))
    // split when shp or dbf reaches 100MB
    val maxSize = 100 * 1024 * 1024
    dumper.setMaxDbfSize(maxSize)
    dumper.setMaxDbfSize(maxSize)
    // actually dump data
    val fc: DefaultFeatureCollection = new DefaultFeatureCollection()
    val sq_sf: Seq[SimpleFeature] =
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
    *
    * @param library_shp_filepath
    * @param query_shp_filepath
    */
  def readShapefilesAndIntersect(library_shp_filepath: String, query_shp_filepath: String): Unit = {
    val lib_features: Seq[MultiPolygonFeature[Map[String, Object]]] = readMultiPolygonFeatures(library_shp_filepath)
    val qry_features: Seq[MultiPolygonFeature[Map[String, Object]]] = readMultiPolygonFeatures(query_shp_filepath)

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

  def createSpatialKeyFromTiff(multiband_gtiff: MultibandGeoTiff): Seq[SpatialKey] = {
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

  def createSFCIndex(sfc_index_label: String, x_resolution: Int, y_resolution: Int): KeyIndex[SpatialKey] = {

    sfc_index_label match {
      case Constants.SFC_LABEL_HILBERT => {
        val max_int = (math.pow(2, 32) - 1).toInt
        //new HilbertSpatialKeyIndex(KeyBounds(SpatialKey(0, 0), SpatialKey(max, max)), 31, 31)
        println("sfc_index: HILBERT : "+sfc_index_label)

        new HilbertSpatialKeyIndex(
          KeyBounds(
            SpatialKey(0, 0),
            //SpatialKey(max_int, max_int)),
            SpatialKey(max_int, max_int)),
          x_resolution,
          y_resolution)
      }
      case Constants.SFC_LABEL_ZORDER => {
        val max_int = (math.pow(2, 32) - 1).toInt
        println("sfc_index: ZORDER : "+sfc_index_label)

        new ZSpatialKeyIndex(
          KeyBounds(
            SpatialKey(0, 0),
            SpatialKey(x_resolution, y_resolution)))
      }
    }
  }

  def readGeotiffAndTile(file_path: String, output_dir_path: String, sfc_index_label: String)(implicit sc: SparkContext): Unit = {
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

    // Reproject to EPSG:32651
    // TODO: check projection first and reproject only when needed
    val (zoom, reprojected_rdd): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
    MultibandTileLayerRDD(tiled_rdd, rasterMetaData)
      .reproject(CRS.fromEpsgCode(32651), FloatingLayoutScheme(TILE_SIZE), Bilinear)
    val final_crs = CRS.fromEpsgCode(32651)

    val sfc_index = createSFCIndex(sfc_index_label,
      reprojected_rdd.keys.max()._1,
      reprojected_rdd.keys.max()._2)

    println("Created SFC index: "+sfc_index.toString)

    val sfc_indexed_rdd: RDD[(SpatialKey, Extent)] = reprojected_rdd
      // Filter out negative keys first to prevent errors
      .filter(x => (x._1._1 >= 0) && (x._1._2 >= 0) )
      .map{ tup =>
        val (spatial_key:SpatialKey, raster_tile:MultibandTile) = tup
        val extent : Extent = spatial_key.extent(rasterMetaData.layout)
        println("Indexing Spatial Key: "+spatial_key)
        val sfc_index_hex = sfc_index.toIndex(spatial_key).toHexString
        val padded_hex = padLeft(sfc_index_hex, Constants.PAD_LENGTH, '0')
        //val gtif_file_path : String = output_dir_path+"_"+hilbert_hex+".tif"
        val gtif_file_path : String = os_path_join(output_dir_path,padded_hex+"_"+spatial_key._1+"_"+spatial_key._2+".tif")

        Holder.log.debug(s"Cutting $SpatialKey of ${raster_tile.dimensions} cells covering $extent to [$gtif_file_path]")
        // Write tile to GeoTiff file
        GeoTiff(raster_tile, extent, final_crs).write(gtif_file_path)
        (spatial_key:SpatialKey, extent)
        //TODO: Create Vector Layer outline of tiles using extents (reduce step)
      }
    println("Tiled RDD Items: " + tiled_rdd.count())
    println("Result RDD Items: " + sfc_indexed_rdd.count())
    println("Space Filling Curve Dimensions: " + reprojected_rdd.keys.max())
    pprint.pprintln(sfc_indexed_rdd)
  }

  def readGeotiffAndTileWithHilbert(file_path: String, output_dir_path: String)(implicit sc: SparkContext): Unit = {
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

//    val resampleMethod = NearestNeighbor
//    val mapTransform = rasterMetaData.layout.mapTransform

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

    val sfc_indexed_rdd: RDD[(SpatialKey, Extent)] = reprojected_rdd
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
    println("Result RDD Items: " + sfc_indexed_rdd.count())
    println("Hilbert Dimensions: " + reprojected_rdd.keys.max())
    pprint.pprintln(sfc_indexed_rdd)
  }

  def simpleReadTileQuery(run_rep: Int, src_raster_file_path: String, tile_out_path: String, meta_shp_path : String, qry_shp_path : String, output_gtif_path : String )
                   (implicit spark_s: SparkSession): Unit ={
    val stg_metrics_tile = new ch.cern.sparkmeasure.StageMetrics(spark_s)

    println("SIMPLE RUN REP: "+run_rep.toString)

    val (
      reprojected_rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]],
      raster_metadata) = stg_metrics_tile.runAndMeasure(
      simpleReadGeotiffAndTile(src_raster_file_path, tile_out_path)
    )

    val stg_metrics_query_tiles = new ch.cern.sparkmeasure.StageMetrics(spark_s)

    val filtered_tile_rdd_with_meta = stg_metrics_query_tiles.runAndMeasure(
      simpleQueryTilesWithShp(reprojected_rdd, qry_shp_path, output_gtif_path)
    )

    val stg_metrics_query_meta = new ch.cern.sparkmeasure.StageMetrics(spark_s)
    stg_metrics_query_meta.runAndMeasure(
      simpleQueryMetadataWithShp(filtered_tile_rdd_with_meta, meta_shp_path, output_gtif_path)
    )

  }

  def simpleReadGeotiffAndTile(file_path: String, tile_out_path: String)
                              (implicit spark_s: SparkSession): (RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]], TileLayerMetadata[SpatialKey])= {
    implicit val sc = spark_s.sparkContext
    val extent_tile_rdd: RDD[(ProjectedExtent, MultibandTile)] =
      sc.hadoopMultibandGeoTiffRDD(file_path)

    // Tiling layout to TILE_SIZE x TILE_SIZE grids
    val (_, rasterMetaData) =
      TileLayerMetadata.fromRdd(extent_tile_rdd, FloatingLayoutScheme(TILE_SIZE))

    val tiled_rdd: RDD[(SpatialKey, MultibandTile)] =
      extent_tile_rdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
        .repartition(RDD_PARTS)

    // ### Reproject
    val (zoom, reprojected_rdd): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
      MultibandTileLayerRDD(tiled_rdd, rasterMetaData)
        .reproject(CRS.fromEpsgCode(32651), FloatingLayoutScheme(TILE_SIZE), Bilinear)
    val final_crs = CRS.fromEpsgCode(32651)

    // Write tiles to dir
//    reprojected_rdd.map{ tup =>
//        val (spatial_key:SpatialKey, raster_tile:MultibandTile) = tup
//        val extent : Extent = spatial_key.extent(rasterMetaData.layout)
//        println("Indexing Spatial Key: "+spatial_key)
//        //val gtif_file_path : String = output_dir_path+"_"+hilbert_hex+".tif"
//        val gtif_file_path : String = os_path_join(tile_out_path,spatial_key._1+"_"+spatial_key._2+".tif")
//
//
//        Holder.log.debug(s"Cutting $SpatialKey of ${raster_tile.dimensions} cells covering $extent to [$gtif_file_path]")
//        // Write tile to GeoTiff file
//        GeoTiff(raster_tile, extent, final_crs).write(gtif_file_path)
//      }

    println("SPATIAL KEYS:")
    reprojected_rdd.collect().foreach(
    { tup =>
      val (spatial_key:SpatialKey, raster_tile:MultibandTile) = tup
      println("   ["+spatial_key._1+"_"+spatial_key._2+"]")
    })


//    return (tiled_rdd, rasterMetaData)
    return (reprojected_rdd, rasterMetaData)
  }

  def simpleQueryTilesWithShp(tiled_rdd_meta: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]], qry_shp_path : String, output_gtif_path : String )
                             (implicit spark_s: SparkSession): RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] ={
    implicit val sc = spark_s.sparkContext
    val features = ShapeFileReader.readMultiPolygonFeatures(qry_shp_path)
    val region: MultiPolygon = features(0).geom
    val query_extents = features(0).envelope
    val attribute_table = features(0).data
    val filtered_rdd = tiled_rdd_meta.filter().where(Intersects(query_extents)).result

    val raster_tile: Raster[MultibandTile] = filtered_rdd.mask(region).stitch // Correct so far
    println("EXECUTOR MEMORY: "+sc.getExecutorMemoryStatus)
    GeoTiff(raster_tile, tiled_rdd_meta.metadata.crs).write(output_gtif_path)


    return filtered_rdd
  }

  def simpleQueryMetadataWithShp(tiled_rdd_meta: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]], meta_shp_path : String, output_gtif_path : String)
                                (implicit spark_s: SparkSession){

    implicit val sc = spark_s.sparkContext
    val metadata_fts : Seq[MultiPolygonFeature[Map[String,Object]]] = ShapeFileReader.readMultiPolygonFeatures(meta_shp_path)
    val metadata_fts_rdd : RDD[MultiPolygonFeature[Map[String,Object]]]= spark_s.sparkContext.parallelize(metadata_fts, RDD_PARTS)

    // Get extents and combine to a multipolygon
    val mapTransform = tiled_rdd_meta.metadata.getComponent[LayoutDefinition].mapTransform
    val tile_extents_mp =  MultiPolygon(tiled_rdd_meta.map {
      case (k, tile) =>
        val key = k.getComponent[SpatialKey]
        mapTransform(key).toPolygon()
    }.collect())


//    extent_tile_rdd.map{
//      case (prj_extent,multiband_tile) =>
//
//    }

//    val filtered_metadata_fts = metadata_fts_rdd.aggregate[RDD[MultiPolygonFeature[Map[String,Object]]]](RDD[MultiPolygonFeature[Map[String,Object]]])(
//      {(acc, item) =>
//        if(item.intersects(tile_extents_mp)){
//          acc ++ sc.parallelize(Seq(item))
//        }else{
//          acc
//        }
//      },
//      {(acc1, acc2) =>
//        acc1 ++ acc1
//      })

    val json_serializer = Json(DefaultFormats)

    val meta_json_string : String = metadata_fts_rdd.aggregate[String]("")(
      {(acc, cur_ft) =>
        if(cur_ft.intersects(tile_extents_mp)){
          val map_json = Json(DefaultFormats).write(cur_ft.data)
          acc + ",\n" + map_json
        }else{
          acc
        }
      },
      {(acc1, acc2) =>
        if(acc1.length > 0 && acc2.length > 0) {
          acc1 + ",\n" + acc2
        }else{
          acc1 + acc2
        }
      })

    new PrintWriter(
      output_gtif_path.replaceAll("\\.[^.]*$", "") + ".json")
    {
      write(meta_json_string + "\n"); close }
  }

  def invertHexIndex(hex_string :String, x_resolution: Int, y_resolution: Int, sfc_index_label: String): SpatialKey = {
    val long_index = java.lang.Long.parseLong(hex_string, 16)

    sfc_index_label match {
      case Constants.SFC_LABEL_HILBERT => {
        return SpatialKey(Z2.combine(long_index)+1, (Z2.combine(long_index>>1))+1)
      }
      case Constants.SFC_LABEL_ZORDER => {
        val chc = {
          val dimensionSpec =
            new MultiDimensionalSpec(
              List(
                x_resolution+1,
                y_resolution+1
              ).map(new java.lang.Integer(_)).asJava
            )

          new CompactHilbertCurve(dimensionSpec)
        }
        val bit_vectors =
          Array(
            BitVectorFactories.OPTIMAL.apply(x_resolution+1),
            BitVectorFactories.OPTIMAL.apply(y_resolution+1)
          )
        val long_var = java.lang.Long.parseLong(hex_string, 16)
        val hilbertBitVector = BitVectorFactories.OPTIMAL.apply(chc.getSpec.sumBitsPerDimension)
        hilbertBitVector.copyFrom(long_var)
        chc.indexInverse(hilbertBitVector, bit_vectors)
        return SpatialKey(bit_vectors(0).toLong.toInt ,bit_vectors(1).toLong.toInt)
      }
    }

  }

  /**
    * TODO: create invert function for Z Order index
    * @param hex_string
    * @param xResolution
    * @param yResolution
    * @return
    */
  def invertHilbertIndex_refactor(hex_string :String, xResolution: Int, yResolution: Int): Array[BitVector] = {
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


  def readTiles(tile_dir_path: String, output_gtif_path: String, sfc_index_label: String)(implicit spark_s: SparkSession)={
    // Create Sequence of Geotiffs
    implicit val sc = spark_s.sparkContext
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
    // Get x,y resolution from list of tile file names

    val XYList = tif_file_list.map{
      tif_file =>
        tif_file.getName.split(".")(0).split("_").slice(1,2)
    }

    val hilbert_index = {
      import geotrellis.spark.io.index.hilbert._
      val max_int = (math.pow(2, 32) - 1).toInt
      //new HilbertSpatialKeyIndex(KeyBounds(SpatialKey(0, 0), SpatialKey(max, max)), 31, 31)

      new HilbertSpatialKeyIndex(
        KeyBounds(GridBounds(0,0,max_int,max_int)),
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

        val decoded_spatial_key = invertHexIndex(hex_hilbert,xResolution,yResolution, sfc_index_label)
        pprint.pprintln(filename + " | "
          + decoded_spatial_key._1 + " , "+ decoded_spatial_key._2
          + " | " + mband_gtif.extent)

        val mband_tile = mband_gtif.raster.tile

        if( decoded_spatial_key.equals(hilbert_index.keyBounds.minKey)){
          x_vals += mband_gtif.extent.xmin
          y_vals += mband_gtif.extent.ymax
        }

        if( decoded_spatial_key.equals(hilbert_index.keyBounds.maxKey)){
          x_vals += mband_gtif.extent.xmax
          y_vals += mband_gtif.extent.ymin
        }

        (decoded_spatial_key,mband_tile)
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


  def queryTiles(tile_dir_path: String, query_shp: String, output_gtif_path: String, sfc_index_label : String)(implicit spark_s: SparkSession)= {
    // Create Sequence of Geotiffs
    implicit val sc = spark_s.sparkContext
    val tif_file_list = getListOfFiles(tile_dir_path, List[String]("tif"))
    val gtif_list: List[(String, MultibandGeoTiff)] = tif_file_list.map { tif_file =>
      (tif_file.getName, GeoTiffReader.readMultiband(tif_file.getAbsolutePath)) //.raster.tile)
    }

    println("sizeEstimate - gtif_list: "+SizeEstimator.estimate(gtif_list).toString)

    val tile_crs: geotrellis.proj4.CRS = gtif_list(0)._2.crs

    // Get x,y resolution from list of tile file names
    val xy_list = tif_file_list.map {
      tif_file =>
        tif_file.getName.split('.')(0).split('_').drop(1).map(_.toInt)
    }
    val xy_rdd = sc.parallelize(xy_list)

    println("sizeEstimate - xy_rdd: "+SizeEstimator.estimate(xy_list).toString)

    val min_max_x = xy_rdd.aggregate[(Int,Int)](xy_list(0)(0),xy_list(0)(0))(
        { (acc, item) =>   // Combining accumulator and element
                    (math.min(acc._1, item(0)), math.max(acc._2, item(0)))
                  },
        { (acc1, acc2) =>   // Combining accumulator and element
                    (math.min(acc1._1, acc2._1), math.max(acc1._2, acc2._2))
        }

    )

    val min_max_y = xy_list.aggregate[(Int,Int)](xy_list(0)(1),xy_list(0)(1))(
      { (acc, item) =>   // Combining accumulator and element
        (math.min(acc._1, item(1)), math.max(acc._2, item(1)))
      },
      { (acc1, acc2) =>   // Combining accumulator and element
        (math.min(acc1._1, acc2._1), math.max(acc1._2, acc2._2))
      }

    )
    println("Min/Max XY:")
    pprint.pprintln(min_max_x)
    pprint.pprintln(min_max_y)
    val x_resolution = min_max_x._2
    val y_resolution = min_max_y._2

    val sfc_index = createSFCIndex(sfc_index_label, x_resolution, y_resolution)

    val x_vals = scala.collection.mutable.ArrayBuffer.empty[Double]
    val y_vals = scala.collection.mutable.ArrayBuffer.empty[Double]

    val mtl_seq : Seq[(SpatialKey,MultibandTile)] = gtif_list.map {
      list_item =>
        val filename : String  = list_item._1
        val hex_hilbert = filename.split("_")(0)
        val mband_gtif: MultibandGeoTiff = list_item._2
        
//        pprint.pprintln(filename + " | "
//          + bitvectors(0).toLong + " , "+ bitvectors(1).toLong
//          + " | " + mband_gtif.extent)

        val decoded_spatial_key = invertHexIndex(hex_hilbert,x_resolution,y_resolution, sfc_index_label)
        val mband_tile = mband_gtif.raster.tile

        if( decoded_spatial_key.equals(sfc_index.keyBounds.minKey)){
          x_vals += mband_gtif.extent.xmin
          x_vals += mband_gtif.extent.xmax
          y_vals += mband_gtif.extent.ymin
          y_vals += mband_gtif.extent.ymax
        }

        if( decoded_spatial_key.equals(sfc_index.keyBounds.maxKey)){
          x_vals += mband_gtif.extent.xmin
          x_vals += mband_gtif.extent.xmax
          y_vals += mband_gtif.extent.ymin
          y_vals += mband_gtif.extent.ymax
        }

        (decoded_spatial_key,mband_tile)
    }

    val raster_merged_extents = new Extent(x_vals.min, y_vals.min, x_vals.max, y_vals.max)
    println("Extents: "+raster_merged_extents.toString())

    // Create MultibandLayerRDD[K,V] with Metadata from RDD
    val mtl_rdd : RDD[(SpatialKey,MultibandTile)] = sc.parallelize(mtl_seq)
    println("sizeEstimate - mtl_rdd: "+SizeEstimator.estimate(mtl_rdd).toString)

    val recreated_metadata = TileLayerMetadata(
      gtif_list(0)._2.cellType,
      LayoutDefinition(
        GridExtent(
          raster_merged_extents,
          10.0, //thesis.Constants.TILE_SIZE.toDouble,
          10.0 //thesis.Constants.TILE_SIZE.toDouble
        ),
        thesis.Constants.TILE_SIZE,
        thesis.Constants.TILE_SIZE
      ),
      raster_merged_extents,
      tile_crs,
      sfc_index.keyBounds)
    val rdd_with_meta = ContextRDD(mtl_rdd, recreated_metadata)
    println("CREATED METADATA: ")
    println("sizeEstimate - rdd_with_meta: "+SizeEstimator.estimate(rdd_with_meta).toString)
    pprint.pprintln(recreated_metadata)

    //Stitches the raster together
//    val raster_tile: MultibandTile = rdd_with_meta.stitch()
//    GeoTiff(raster_tile, raster_merged_extents, tile_crs).write(output_gtif_path)

    val features = ShapeFileReader.readMultiPolygonFeatures(query_shp)
    val region: MultiPolygon = features(0).geom
    val query_extents = features(0).envelope
    val attribute_table = features(0).data
    println("sizeEstimate - features: "+SizeEstimator.estimate(features).toString)


    //    val raster_tile: MultibandTile = rdd_with_meta.stitch().mask(region) // Correct so far
//    GeoTiff(raster_tile, raster_merged_extents, tile_crs).write(output_gtif_path)
    val filtered_rdd = rdd_with_meta.filter().where(Intersects(query_extents)).result

    val raster_tile: Raster[MultibandTile] = filtered_rdd.mask(region).stitch // Correct so far

    println("EXECUTOR MEMORY: "+sc.getExecutorMemoryStatus)
    println("sizeEstimate - raster_tile: "+SizeEstimator.estimate(raster_tile).toString)

    GeoTiff(raster_tile, tile_crs).write(output_gtif_path)

    //    val raster_tile: MultibandTile = rdd_with_meta.stitch().mask(region).crop(query_extents) // Correct so far
//    GeoTiff(raster_tile, query_extents, tile_crs).write(output_gtif_path)

//    val raster_tile: Raster[MultibandTile] = rdd_with_meta.mask(region).stitch() // To be tested
//    GeoTiff(raster_tile, tile_crs).write(output_gtif_path)


  }

  def compareMetadata(tile_dir_path :String, merged_tif_path:String, sfc_index_label: String)(implicit spark_s: SparkSession)={
    // Create Sequence of Geotiffs
    implicit val sc = spark_s.sparkContext
    val tif_file_list = getListOfFiles(tile_dir_path, List[String]("tif"))
    val gtif_list: List[(String, MultibandGeoTiff)] = tif_file_list.map { tif_file =>
      (tif_file.getName, GeoTiffReader.readMultiband(tif_file.getAbsolutePath)) //.raster.tile)
    }

    val tile_crs: geotrellis.proj4.CRS = gtif_list(0)._2.crs

    // Get x,y resolution from list of tile file names
    val xy_list = tif_file_list.map {
      tif_file =>
        tif_file.getName.split('.')(0).split('_').drop(1).map(_.toInt)
    }
    val xy_rdd = sc.parallelize(xy_list)

    val min_max_x = xy_rdd.aggregate[(Int,Int)](xy_list(0)(0),xy_list(0)(0))(
      { (acc, item) =>   // Combining accumulator and element
        (math.min(acc._1, item(0)), math.max(acc._2, item(0)))
      },
      { (acc1, acc2) =>   // Combining accumulator and element
        (math.min(acc1._1, acc2._1), math.max(acc1._2, acc2._2))
      }

    )

    val min_max_y = xy_list.aggregate[(Int,Int)](xy_list(0)(1),xy_list(0)(1))(
      { (acc, item) =>   // Combining accumulator and element
        (math.min(acc._1, item(1)), math.max(acc._2, item(1)))
      },
      { (acc1, acc2) =>   // Combining accumulator and element
        (math.min(acc1._1, acc2._1), math.max(acc1._2, acc2._2))
      }

    )

    pprint.pprintln(min_max_x)
    pprint.pprintln(min_max_y)
    val xResolution = min_max_x._2
    val yResolution = min_max_y._2

    val hilbert_index = {
      val max_int = (math.pow(2, 32) - 1).toInt
      //new HilbertSpatialKeyIndex(KeyBounds(SpatialKey(0, 0), SpatialKey(max, max)), 31, 31)

      new HilbertSpatialKeyIndex(
        KeyBounds(GridBounds(0, 0, xResolution, yResolution)),
        xResolution, yResolution)
    }

    val x_vals = scala.collection.mutable.ArrayBuffer.empty[Double]
    val y_vals = scala.collection.mutable.ArrayBuffer.empty[Double]

    val mtl_seq : Seq[(SpatialKey,MultibandTile)] = gtif_list.map {
      list_item =>
        val filename : String  = list_item._1
        val hex_hilbert = filename.split("_")(0)
        val mband_gtif: MultibandGeoTiff = list_item._2

        val decoded_spatial_key = invertHexIndex(hex_hilbert,xResolution,yResolution, sfc_index_label)
        pprint.pprintln(filename + " | "
          + decoded_spatial_key._1 + " , "+ decoded_spatial_key._2
          + " | " + mband_gtif.extent)
        val mband_tile = mband_gtif.raster.tile

        if( decoded_spatial_key.equals(hilbert_index.keyBounds.minKey)){
          x_vals += mband_gtif.extent.xmin
          x_vals += mband_gtif.extent.xmax
          y_vals += mband_gtif.extent.ymin
          y_vals += mband_gtif.extent.ymax
        }

        if( decoded_spatial_key.equals(hilbert_index.keyBounds.maxKey)){
          x_vals += mband_gtif.extent.xmin
          x_vals += mband_gtif.extent.xmax
          y_vals += mband_gtif.extent.ymin
          y_vals += mband_gtif.extent.ymax
        }

        (decoded_spatial_key,mband_tile)
    }

    val raster_merged_extents = new Extent(x_vals.min, y_vals.min, x_vals.max, y_vals.max)
    println("Extents: "+raster_merged_extents.toString())

    // Create MultibandLayerRDD[K,V] with Metadata from RDD
    val mtl_rdd : RDD[(SpatialKey,MultibandTile)] = sc.parallelize(mtl_seq)


    //TODO: Ongoing here
    val recreated_metadata = TileLayerMetadata(
      gtif_list(0)._2.cellType,
      LayoutDefinition(
        GridExtent(
          raster_merged_extents,
          raster_merged_extents.width/(thesis.Constants.TILE_SIZE*(hilbert_index.xResolution+1)).toDouble, //thesis.Constants.TILE_SIZE.toDouble,
          raster_merged_extents.height/(thesis.Constants.TILE_SIZE*(hilbert_index.yResolution+1)).toDouble //thesis.Constants.TILE_SIZE.toDouble
        ),
        thesis.Constants.TILE_SIZE,
        thesis.Constants.TILE_SIZE
      ),
      raster_merged_extents,
      tile_crs,
      hilbert_index.keyBounds)

    println(">>> Reading GeoTiff: "+merged_tif_path)
    val input_rdd: RDD[(ProjectedExtent, MultibandTile)] =
      sc.hadoopMultibandGeoTiffRDD(merged_tif_path)

    println(">>> Tiling GeoTiff")
    // Tiling layout to TILE_SIZE x TILE_SIZE grids
    val (_, merged_raster_metadata) =
      TileLayerMetadata.fromRdd(input_rdd, FloatingLayoutScheme(TILE_SIZE))
    println("TILED: ")
    pprint.pprintln(recreated_metadata)
    println("MERGED: ")
    pprint.pprintln(merged_raster_metadata)
  }
}
