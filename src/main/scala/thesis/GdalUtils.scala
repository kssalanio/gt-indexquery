package thesis

import org.gdal.gdal
import org.gdal.gdal.Dataset
import org.gdal.gdalconst.gdalconstConstants

object GdalUtils {
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

}
