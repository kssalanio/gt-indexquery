package thesis

import java.io._
import java.text.SimpleDateFormat
import java.util.Calendar

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
import geotrellis.spark.io.Intersects
import geotrellis.spark.io.index.hilbert.HilbertSpatialKeyIndex
import geotrellis.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

import scala.reflect.ClassTag
import thesis.Constants._
import thesis.InProgress.{createSFCIndex, invertHexIndex}
import thesis.ThesisUtils._


object Refactored {

  def readAndMergeTiles(tile_dir_path: String, output_gtif_path: String, sfc_index_label: String)(implicit spark_s: SparkSession)={
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


  def readGeotiffAndTile(dataset_uid: String, file_path: String, output_dir_path: String, sfc_index_label: String)(implicit sc: SparkContext): Unit = {
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
        val gtif_file_path : String = os_path_join(output_dir_path,dataset_uid+"_"+padded_hex+"_"+spatial_key._1+"_"+spatial_key._2+".tif")

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


  def createTileMetadata(dataset_uid :String, tile_dir_path: String, metadata_shp_filepath :String)(implicit spark_s : SparkSession) = {
    val metadata_fts : Seq[MultiPolygonFeature[Map[String,Object]]] = ShapeFileReader.readMultiPolygonFeatures(metadata_shp_filepath)
    val metadata_fts_rdd : RDD[MultiPolygonFeature[Map[String,Object]]]= spark_s.sparkContext.parallelize(metadata_fts, RDD_PARTS)

    println("sizeEstimate - Metadata SHP: "+SizeEstimator.estimate(metadata_fts).toString)

    val gtiff_files = getListOfFiles(tile_dir_path,List[String]("tif"))
    val mband_gtiffs : List[(File, MultibandGeoTiff)] = gtiff_files.map { tif_file =>
      (tif_file,GeoTiffReader.readMultiband(tif_file.getAbsolutePath))
    }

    println("sizeEstimate - MBand Gtiffs: "+SizeEstimator.estimate(mband_gtiffs).toString)


    val merged_jsons = mband_gtiffs.map{
      list_item =>
        val (tif_file, gtiff) = list_item
        //TODO: Handle empty intersection (no features intersecting the tile)
        val merged_map_json : String = createGeoTiffMetadataJSON_withDSUid(dataset_uid,tif_file.getName, gtiff, metadata_fts_rdd)
        val json_dir : File = new File(os_path_join(tile_dir_path,"json"))
        if (!json_dir.exists) json_dir.mkdir

        new PrintWriter(
//          tif_file.getAbsoluteFile.toString.replaceAll("\\.[^.]*$", "") + ".json")
          os_path_join(json_dir.getAbsolutePath, tif_file.getName.replaceAll("\\.[^.]*$", "") + ".json"))
        {
          write(merged_map_json); close }
        merged_map_json
    }

    println("sizeEstimate - merged_jsons: "+SizeEstimator.estimate(merged_jsons).toString)
  }


  def createGeoTiffMetadataJSON_withDSUid(dataset_uid :String,tif_filename:String, tif_file: MultibandGeoTiff, metadata_fts_rdd: RDD[MultiPolygonFeature[Map[String, Object]]])
                                         (implicit spark_s : SparkSession): String = {
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
    //val json_files = getListOfFiles(tile_json_dir_path,List[String]("json"))
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
            }.map(keyword => (keyword, path, tile_code, tile_dataset_uid))

          /**
            * Should be:
            *
            * }.map(keyword => (keyword, keyword_recursive_list, path, tile_code, tile_dataset_uid))
            *
            * Where <keyword_recursive_list> is the list of keywords
            *   for nested dictionaries or maps. Defaults to <keyword>
            *   if it is not nested
            */

        }
      }
    }.map {
      case (keyword, path, tile_code, tile_dataset_uid) => (keyword, (path, tile_code, tile_dataset_uid))
//      case (keyword, path, tile_code, tile_dataset_uid) => ((keyword, path), (tile_code, tile_dataset_uid))
//      case (keyword, path, tile_code, tile_dataset_uid) => (path, keyword)
    }.distinct
    .groupByKey
    .mapValues(iterator => iterator.mkString(", "))
    .saveAsTextFile(tile_dir_path+"/inverted_idx_"+run_rep)
  }

  def loadInvertedIndex(invidx_file_path: String)(implicit spark_s : SparkSession): RDD[(String, Array[String])] ={
    val invidx_file = spark_s.sparkContext.textFile(invidx_file_path)
    val inverted_idx_rdd = invidx_file.map { text_line =>
      val tokens = text_line.split(",",2)

      //Filter tokens here before any other post processing
      val file_list = tokens(1).drop(1).dropRight(2).split("\\), \\(").map{
        //      val file_list = tokens(1).dropRight(1).split("\\), \\(").map{
        file_item =>
          file_item.replaceAll("\\s", "").split(",")
      }
      val file_paths = tokens(1).drop(1).dropRight(2).split("\\), \\(").map{
        file_item =>
          file_item.replaceAll("\\s", "").split(",")(0)
      }

      (tokens(0).drop(1), file_paths)
    }
    return inverted_idx_rdd
  }



  def searchInvertedIndex(invidx_file_path: String, search_tags: Array[String])(implicit spark_s : SparkSession): Array[String] ={
    val search_tag_seq: Seq[SearchTag] = search_tags.grouped(3)
      .map{ tag_item =>
        new SearchTag(tag_item(0), tag_item(1), tag_item(2))
      }.toList

    // Load Inverted Index from file into a filter-able RDD
    val invidx_rdd = loadInvertedIndex(invidx_file_path)

    val keyword_list = search_tag_seq.map{
      search_tag :SearchTag =>
        search_tag.tag
    }

    // Search Inverted Index for files with search keywords
    val metadocs_with_keywords = invidx_rdd.filter{
      case(keyword, documents) =>
        keyword_list.contains(keyword)
    }.map{case(keyword, documents) => documents.toSet}
      .reduce(_.intersect(_))
      .map{
        document_name =>
          document_name.split(":")(1)
      }
    println("FILTERED:")
    metadocs_with_keywords.foreach( item=>
      pprint.pprintln(item)
    )

    /**
      * TODO:
      * - Filtered only by presence of keyword
      * - Need to eval each tag with operator and value found in each document
      */

    return metadocs_with_keywords.toArray
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

    sc.parallelize(tif_file_list)

    // Get x,y resolution from list of tile file names
    val xy_rdd = tif_file_list.map {
      tif_file =>
        println("Handling tif file: "+tif_file.getName)
        tif_file.getName.split('.')(0).split('_').drop(2).map(_.toInt)
    }
//    val xy_rdd = sc.parallelize(xy_list)

    println("sizeEstimate - xy_rdd: "+SizeEstimator.estimate(xy_rdd).toString)

    val min_max_x = xy_rdd.aggregate[(Int,Int)](xy_rdd(0)(0),xy_rdd(0)(0))(
      { (acc, item) =>   // Combining accumulator and element
        (math.min(acc._1, item(0)), math.max(acc._2, item(0)))
      },
      { (acc1, acc2) =>   // Combining accumulator and element
        (math.min(acc1._1, acc2._1), math.max(acc1._2, acc2._2))
      }

    )

    val min_max_y = xy_rdd.aggregate[(Int,Int)](xy_rdd(0)(1),xy_rdd(0)(1))(
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

        // Commented out because it results to empty values
        // when filtered keys aren't in the list
//        if( decoded_spatial_key.equals(sfc_index.keyBounds.minKey)){
//          x_vals += mband_gtif.extent.xmin
//          x_vals += mband_gtif.extent.xmax
//          y_vals += mband_gtif.extent.ymin
//          y_vals += mband_gtif.extent.ymax
//        }
//
//        if( decoded_spatial_key.equals(sfc_index.keyBounds.maxKey)){
//          x_vals += mband_gtif.extent.xmin
//          x_vals += mband_gtif.extent.xmax
//          y_vals += mband_gtif.extent.ymin
//          y_vals += mband_gtif.extent.ymax
//        }
        x_vals += mband_gtif.extent.xmin
        x_vals += mband_gtif.extent.xmax
        y_vals += mband_gtif.extent.ymin
        y_vals += mband_gtif.extent.ymax

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
    println("Query Extents: "+features(0).envelope)



    //    val raster_tile: MultibandTile = rdd_with_meta.stitch().mask(region) // Correct so far
    //    GeoTiff(raster_tile, raster_merged_extents, tile_crs).write(output_gtif_path)
    val filtered_rdd = rdd_with_meta.filter().where(Intersects(region)).result

    //TODO: HERE error in filter and stitch, results to empty
    println("Filtered ")
    println(filtered_rdd.count())
    filtered_rdd.collect.foreach{ tile =>
      println(tile._1)
    }

    val raster_tile: Raster[MultibandTile] = filtered_rdd.mask(region).stitch // Correct so far

    println("EXECUTOR MEMORY: "+sc.getExecutorMemoryStatus)
    println("sizeEstimate - raster_tile: "+SizeEstimator.estimate(raster_tile).toString)

    GeoTiff(raster_tile, tile_crs).write(output_gtif_path)

    //    val raster_tile: MultibandTile = rdd_with_meta.stitch().mask(region).crop(query_extents) // Correct so far
    //    GeoTiff(raster_tile, query_extents, tile_crs).write(output_gtif_path)

    //    val raster_tile: Raster[MultibandTile] = rdd_with_meta.mask(region).stitch() // To be tested
    //    GeoTiff(raster_tile, tile_crs).write(output_gtif_path)


  }
  def queryTilesWithMeta(query_shp: String, output_gtif_path: String, sfc_index_label : String, invidx_file_path: String, search_tags: Array[String])(implicit spark_s : SparkSession)= {
    implicit val sc = spark_s.sparkContext
    val meta_json_docs = searchInvertedIndex(invidx_file_path: String, search_tags: Array[String])
    val tif_file_list = meta_json_docs.map{
      json_doc_path =>
        new File(json_doc_path.replace("/json", "").replace(".json", ".tif"))
    }.toList

    println("GEOTIFF FILES:")
    tif_file_list.foreach( item=>
      pprint.pprintln(item)
    )

    // Create Sequence of Geotiffs
    val gtif_list: List[(String, MultibandGeoTiff)] = tif_file_list.map { tif_file =>
      (tif_file.getName, GeoTiffReader.readMultiband(tif_file.getAbsolutePath)) //.raster.tile)
    }

    println("sizeEstimate - gtif_list: "+SizeEstimator.estimate(gtif_list).toString)

    val tile_crs: geotrellis.proj4.CRS = gtif_list(0)._2.crs

    //TODO: Recreate index from computing query quadtree and prefix tree, not from inverting file index


    // Get x,y resolution from list of tile file names
    val xy_list = tif_file_list.map {
      tif_file =>
        tif_file.getName.split('.')(0).split('_').takeRight(2).map(_.toInt)
    }

    println("XY_LIST")
    pprint.pprintln(xy_list)


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
        println("FILENAME: "+filename)

        //        val hex_hilbert = filename.split("_")(0) // Hex Index at (0)
        val hex_index = filename.split("_")(1)  // Dataset ID at (0), Hex Index at (1)
        val mband_gtif: MultibandGeoTiff = list_item._2

        //        pprint.pprintln(filename + " | "
        //          + bitvectors(0).toLong + " , "+ bitvectors(1).toLong
        //          + " | " + mband_gtif.extent)

        val decoded_spatial_key = invertHexIndex(hex_index,x_resolution,y_resolution, sfc_index_label)
        println("SPATIAL KEY: "+decoded_spatial_key.toString)
        val mband_tile = mband_gtif.raster.tile

//        if( decoded_spatial_key.equals(sfc_index.keyBounds.minKey)){
//          x_vals += mband_gtif.extent.xmax
//          y_vals += mband_gtif.extent.ymin
//          y_vals += mband_gtif.extent.ymax
//          x_vals += mband_gtif.extent.xmin
//        }
//
//        if( decoded_spatial_key.equals(sfc_index.keyBounds.maxKey)){
//          x_vals += mband_gtif.extent.xmin
//          x_vals += mband_gtif.extent.xmax
//          y_vals += mband_gtif.extent.ymin
//          y_vals += mband_gtif.extent.ymax
//        }

        x_vals += mband_gtif.extent.xmax
        y_vals += mband_gtif.extent.ymin
        y_vals += mband_gtif.extent.ymax
        x_vals += mband_gtif.extent.xmin

        (decoded_spatial_key,mband_tile)
    }

    println("X&Y Vals")
//    pprint.pprintln(x_vals)
    x_vals.foreach(println)
//    pprint.pprintln(y_vals)
    y_vals.foreach(println)
    //TODO: Find the root cause as to why the ArrayBuffers are empty


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
