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
import thesis.Constants._
import thesis.Refactored._
import thesis.ThesisUtils._
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
      val ft_name = ft.data("Block_Name").toString
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

  def invertHexIndex(hex_string :String, x_resolution: Int, y_resolution: Int, sfc_index_label: String): SpatialKey = {
    val long_index = java.lang.Long.parseLong(hex_string, 16)

    sfc_index_label match {
      case Constants.SFC_LABEL_ZORDER => {
        return SpatialKey(Z2.combine(long_index), (Z2.combine(long_index>>1)))
      }
      case Constants.SFC_LABEL_HILBERT => {
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

        // TODO: ??? Gets flipped for some reason ???
//        return SpatialKey(bit_vectors(0).toLong.toInt ,bit_vectors(1).toLong.toInt)
        return SpatialKey(bit_vectors(1).toLong.toInt ,bit_vectors(0).toLong.toInt)
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
}
