package thesis

import java.io._

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.spark._
import geotrellis.spark.io.kryo._
import geotrellis.util._
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

import scala.io.StdIn
import thesis.Refactored._
import thesis.InProgress._
//import sext._


object Main{
  //val logger = Logger.getLogger(Main.getClass)

  def bootSentence = "\n\n>>> INITIALIZE <<<\n\n"
  def helloSentence  = "Hello GeoTrellis"

  def log(msg: String){
    println("> "+msg)
  }

  def createSparkContext(): Unit = {
    val conf = new org.apache.spark.SparkConf()
    conf.setMaster("local[*]")
    implicit val sc = geotrellis.spark.util.SparkUtils.createSparkContext("Test console", conf)
  }

  def createAllSparkConf(): SparkConf = {
    /**
      * # -- MEMORY ALLOCATION -- #
        spark.master                   yarn
        spark.driver.memory            512m
        spark.yarn.am.memory           512m
        spark.executor.memory          512m


        # -- MONITORING -- #
        spark.eventLog.enabled            true
        spark.eventLog.dir                hdfs://sh-master:9000/spark-logs
        spark.history.provider            org.apache.spark.deploy.history.FsHistoryProvider
        spark.history.fs.logDirectory     hdfs://sh-master:9000/spark-logs
        spark.history.fs.update.interval  3s
        spark.history.ui.port             18080
        spark.ui.enabled                  true

      */
    new SparkConf()
//      .setMaster("yarn")
      .setMaster("local")
      .setAppName("Thesis")
      .set("spark.serializer",        classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator",  classOf[KryoRegistrator].getName)
      .set("spark.driver.memory",     "1024m")
      .set("spark.yarn.am.memory",    "1024m")
      .set("spark.executor.memory",   "1024m")
      .set("spark.eventLog.enabled",            "true")
      .set("spark.eventLog.dir",                "/home/spark/spark/logs")
//      .set("spark.eventLog.dir",                "hdfs://sh-master:9000/spark-logs")
      .set("spark.history.provider",            "org.apache.spark.deploy.history.FsHistoryProvider")
//      .set("spark.history.fs.logDirectory",     "hdfs://sh-master:9000/spark-logs")
      .set("spark.history.fs.logDirectory",     "/home/spark/spark/logs")
      .set("spark.history.fs.update.interval",  "3s")
      .set("spark.history.ui.port",             "18080")
      .set("spark.ui.enabled",                  "true")

    //.set("spark.default.parallelism", "2")
    //.set("spark.akka.frameSize", "512")
    //.set("spark.kryoserializer.buffer.max.mb", "800")
  }

  def createSparkConf(): SparkConf = {
    new SparkConf()
      .setMaster("local")
      .setAppName("Thesis")
      //.set("spark.default.parallelism", "2")
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
      //.set("spark.akka.frameSize", "512")
      .set("spark.kryoserializer.buffer.max", "800") // Prevent overflow
      //.set("spark.kryoserializer.buffer.mb","128") // Prevent underflow
      //.set("spark.serializer.objectStreamReset",	"100")
  }

  def main(args: Array[String]): Unit = {
    //Initialize
    println(System.getProperty("java.library.path"))
    unsafeAddDir("/usr/lib/jni/")
    println(System.getProperty("java.library.path"))
    org.gdal.gdal.gdal.AllRegister()
    println(bootSentence)

    if (args.length == 0) {
      println("Need CSV argument")
      throw new Exception("Insufficient args!")
    }

    /**
     * Declare spark conf
     */
//    val conf = createAllSparkConf()
//    val conf = createSparkConf()

//    System.setProperty("spark.ui.enabled", "true")
//    System.setProperty("spark.ui.port", "4040")

    // Init spark context
//    implicit val sc = new SparkContext(conf)

    // Init Spark Session
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("Thesis")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[KryoRegistrator].getName)
      .config("spark.kryoserializer.buffer.max.mb", "800") // Prevent overflow
      .config("spark.ui.enabled", "true")
      .config("spark.executor.memory",   "14g")
      .getOrCreate()
    val run_reps = args(1).toInt

    try {
      /**
        * Select test based on first CLI arg
        */
      println("ARGUMENTS:")
      pprint.pprintln(args)

      args(0) match {
        case "csv" => run_csv_tests(
          run_reps, args(2))(sparkSession)
        case "tile_raster" => run_tile_geotiff(
          run_reps, args(2),args(3),args(4))(sparkSession)
        case "map_meta" => run_map_metadata(
          run_reps, args(2),args(3),args(4))(sparkSession)
        case "inverted_idx" => run_create_inverted_index(
          run_reps, args(2))(sparkSession)
        case "query_shp" => run_query_tiles(
          run_reps, args(2),args(3),args(4),args(5))(sparkSession)
        case "cmp_meta" => run_cmp_meta(
          run_reps, args(2),args(3))(sparkSession)
        case "simple" => run_simple_read_tile_query(
          //simpleReadTileQuery(run_rep,src_raster_file_path, tile_out_path, meta_shp_path, qry_shp_path, output_gtif_path)
          run_reps, args(2),args(3),args(4),args(5),args(6))(sparkSession)
        case "read_tiles" => run_tile_reader_tests(
          run_reps, args(2),args(3))(sparkSession)
        case "run_prelim" => run_prelim_tiling_task(
          run_reps, args(2),args(3))(sparkSession)
        case _ => println("ERROR: Invalid first CLI argument")
      }
      println(">>> END OF RUN <<<")

      // Pause to wait to close the spark context,
      // so that you can check out the UI at http://localhost:4040
//      println("\n\nHit [ENTER] to exit.\n\n")
    } finally {
//      sc.stop()
      println("\n\nHit [ENTER] to exit.\n\n")
      if(run_reps == 1){
        StdIn.readLine()
      }
      sparkSession.stop()
    }
  }

  def readTestCSV(csv_filepath : String) : List[Map[String,String]] = {
    val reader = CSVReader.open(new File(csv_filepath))
    return reader.allWithHeaders()
  }


  def run_csv_tests(run_reps :Int, input_csv_filepath : String)(implicit spark_s: SparkSession) = {

    // Create CSV file for output and write headers
    implicit val sc = spark_s.sparkContext

    val output_csv_file_name = "/home/spark/datasets/csv/results/" + getCurrentDateAndTime("yyyy-MMdd-hhmmss") + ".csv"
    val output_csv_file = new File(output_csv_file_name)
    val output_csv_file_writer = CSVWriter.open(output_csv_file)
    output_csv_file_writer.writeRow(List(
      "TEST_LABEL",
      "TIMESTAMP",
      "SRC_RASTER_SIZE",
      "OUT_RASTER_SIZE",
      "QRY_SHP_PTS",
      "MET_SHP_FTS",
      "INDEXING",
      "MASKING_QRY",
      "MASK_STITCH",
      "METADATA_QRY",
      "IDX_SEC",
      "MSK_SEC",
      "STC_SEC",
      "MET_SEC"))


    //val tests_lst : List[Map[String,String]] = readTestCSV(Constants.TESTS_CSV)
    val tests_lst : List[Map[String,String]] = readTestCSV(input_csv_filepath)

    // Loop through each test parameters in the file
    for (test_params <- tests_lst) {
      val test_label = test_params("test_label")
      val src_raster = test_params("src_raster")
      val qry_shp = test_params("qry_shp")
      val metadata_shp = test_params("metadata_shp")
      val test_reps = test_params("reps").toInt

      // Do @test_reps number of tests
      for( a <- 1 to test_reps){

        val time_idx = time{
          readGeoTiffToMultibandTileLayerRDD(src_raster)
        }

        val mtl_rdd : MultibandTileLayerRDD[SpatialKey] = time_idx._1

        val time_msk = time{
          maskRaster(
            mtl_rdd,
            qry_shp)
        }

        val indexed_mtl_rdd : RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]= time_msk._1
        val time_stitch = time{
          indexed_mtl_rdd.stitch()
        }
        val raster: Raster[MultibandTile] = time_stitch._1 //TODO: trim to extent/envelope of vector. Crop stretches it to its own extents


        val time_metamap = time {
          filterMetadataShapefile(
            metadata_shp,
            qry_shp,
            indexed_mtl_rdd.metadata.layout,
            indexed_mtl_rdd.metadata.crs)
        }
        val metadata_fts_rdd = time_metamap._1



        //writeToESRIShp(result_rdd, crs)
        //writeToGeoJSON(result_rdd, crs, "/home/spark/datasets/output/filtered_geojson/")

        // Write Feature UIDs to file
        //    val ft_uids = result_rdd.map(ft =>
        //        ft.data("UID")
        //      ).collect()
        //    Some(new PrintWriter(new File(output_dir+"feature_uids.list")) { write(ft_uids.mkString("\n")); close })

        // Write filtered results to file


        // Write Result to GeoTiff file
        val output_gtif_filepath = "/home/spark/datasets/output/mask/"+test_label+".tif"
        GeoTiff(raster, indexed_mtl_rdd.metadata.crs).write(output_gtif_filepath)


        //TODO:
        /**
          *       1. Master Quad Tree Spatial Key
          *         (a) generate prefix tree
          *       2. Tile Quad Tree
          *         (a) Quad Tree down to the pixel? For clipping to Vector outline?
          *         (b) A faster way than method (a)
          */
        //readTestCSV("/home/spark/datasets/csv/test_params.csv")
        log("SUMMARY:")
        log("Raster indexing time: "+time_idx._2)
        log("Raster masking time: "+time_msk._2)
        log("Vector filtering time: "+time_metamap._2)
        //output_csv_file_writer.writeRow(List("TIMESTAMP", "INDEXING", "MASKING_QRY", "METADATA_QRY"))
        output_csv_file_writer.writeRow(
          List(
            test_label,
            getCurrentDateAndTime("yyyy-MMdd-hhmmss"),
            getFileSize (src_raster, 2.0),
            getFileSize(output_gtif_filepath, 2.0),
            countPointsSHP(qry_shp),
            countFeaturesSHP(metadata_shp),
            time_idx._2,
            time_msk._2,
            time_stitch._2,
            time_metamap._2,
            nanosecToSec(time_idx._2),
            nanosecToSec(time_msk._2),
            nanosecToSec(time_stitch._2),
            nanosecToSec(time_metamap._2)))
      }

      //val test_params = tests_lst(0)

      output_csv_file_writer.close()

    }

    println("\n\n>> END <<\n\n")
  }

  //def run_tile_geotiff(gtiff_raster_path : String, output_dir_path: String)(implicit sc: SparkContext): Unit ={
  def run_prelim_tiling_task(run_reps :Int, gtiff_raster_path : String, output_dir_path: String)(implicit spark_s: SparkSession): Unit = {
    val stageMetrics = new ch.cern.sparkmeasure.StageMetrics(spark_s)
    val guimaras_raster_path = "/home/spark/datasets/SAR/geonode_sar_guimaras.tif"
    stageMetrics.runAndMeasure(
      readGeotiffAndTile(guimaras_raster_path, output_dir_path, "hilbert")(spark_s.sparkContext))
  }

  def run_tile_geotiff(run_reps :Int, gtiff_raster_path : String, output_dir_path: String, sfc_index_label: String)(implicit spark_s: SparkSession): Unit ={
      val stageMetrics = new ch.cern.sparkmeasure.StageMetrics(spark_s)
//    val time_idx = time{
//      readGeotiffAndTile(gtiff_raster_path, output_dir_path)
//    }
//    log("Raster indexing time: "+time_idx._2)
//    val guimaras_raster_path = "/home/spark/datasets/SAR/geonode_sar_guimaras.tif"
//    stageMetrics.runAndMeasure(
//        readGeotiffAndTile(guimaras_raster_path, output_dir_path)(spark_s.sparkContext))
    for( a <- 1 to run_reps) {
      stageMetrics.runAndMeasure(
        readGeotiffAndTile(gtiff_raster_path, output_dir_path, sfc_index_label)(spark_s.sparkContext))
    }
  }

  def run_map_metadata(run_reps :Int, dataset_uid: String, tile_dir_path: String, metadata_shp_filepath: String)(implicit spark_s: SparkSession): Unit ={
    val stageMetrics = new ch.cern.sparkmeasure.StageMetrics(spark_s)
    for( a <- 1 to run_reps) {
      stageMetrics.runAndMeasure(
        createTileMetadata(dataset_uid, tile_dir_path, metadata_shp_filepath)
//        createTileMetadata_2(dataset_uid, tile_dir_path, metadata_shp_filepath)
      )
    }
  }

  def run_create_inverted_index(run_reps :Int, tile_dir_path: String)(implicit spark_s: SparkSession): Unit ={
    val stageMetrics = new ch.cern.sparkmeasure.StageMetrics(spark_s)
    for( run_rep <- 1 to run_reps) {
      stageMetrics.runAndMeasure(
//        createInvertedIndex(tile_dir_path, run_rep)
        createInvertedIndex_2(tile_dir_path, run_rep)
      )
    }
  }

  def run_tile_reader_tests(run_reps :Int, tile_dir_path: String, output_gtif_path: String)(implicit spark_s: SparkSession) = {
    val stageMetrics = new ch.cern.sparkmeasure.StageMetrics(spark_s)
    for( run_rep <- 1 to run_reps) {
      stageMetrics.runAndMeasure(
        readTiles(tile_dir_path, output_gtif_path)
//    readTiles_v2(tile_dir_path, output_gtif_path)
      )
    }
  }

  def run_query_tiles(run_reps :Int, tile_dir_path: String, query_shp: String, output_gtif_path:String, sfc_index_label: String)(implicit spark_s: SparkSession) = {
    val stageMetrics = new ch.cern.sparkmeasure.StageMetrics(spark_s)
    for( run_rep <- 1 to run_reps) {
      stageMetrics.runAndMeasure(
        queryTiles(tile_dir_path, query_shp, output_gtif_path, sfc_index_label)
        //    readTiles_v2(tile_dir_path, output_gtif_path)
      )
    }
  }

  def run_cmp_meta(run_reps :Int, tile_dir_path: String, merged_tif_path: String) (implicit spark_s: SparkSession) = {
    val stageMetrics = new ch.cern.sparkmeasure.StageMetrics(spark_s)
    for( run_rep <- 1 to run_reps) {
      stageMetrics.runAndMeasure(
        compareMetadata(tile_dir_path, merged_tif_path)
      )
    }
  }

  def run_simple_read_tile_query(run_reps: Int, src_raster_file_path: String, tile_out_path: String, meta_shp_path : String, qry_shp_path : String, output_gtif_path : String )
                                (implicit spark_s: SparkSession) = {
    for( run_rep <- 1 to run_reps) {
      simpleReadTileQuery(run_rep,src_raster_file_path, tile_out_path, meta_shp_path, qry_shp_path, output_gtif_path)
    }
  }

}

object Holder extends Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)
}
