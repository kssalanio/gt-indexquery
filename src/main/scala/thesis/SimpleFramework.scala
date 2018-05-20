package thesis

import java.io.PrintWriter

import geotrellis.proj4.CRS
import geotrellis.raster.{MultibandTile, Raster}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.{Metadata, MultibandTileLayerRDD, SpatialKey, TileLayerMetadata}
import geotrellis.spark.io.Intersects
import geotrellis.spark.tiling.{FloatingLayoutScheme, LayoutDefinition}
import geotrellis.vector.{MultiPolygon, MultiPolygonFeature, ProjectedExtent}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import thesis.Constants._

object SimpleFramework {
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


}
