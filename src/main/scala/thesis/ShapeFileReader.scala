package thesis

/***
  * Copied from geotrellis 2.0
  *
 */
import geotrellis.vector._

import org.geotools.data.simple._
import org.opengis.feature.simple._
import org.geotools.data.shapefile._
import com.vividsolutions.jts.{geom => jts}

import java.net.URL
import java.io.File

import scala.collection.mutable
import scala.collection.JavaConversions._

object ShapeFileReader {
  implicit class SimpleFeatureWrapper(ft: SimpleFeature) {
    def geom[G <: jts.Geometry: Manifest]: Option[G] =
      ft.getAttribute(0) match {
        case g: G => Some(g)
        case _ => None
      }

    def attributeMap: Map[String, Object] =
      ft.getProperties.drop(1).map { p =>
        (p.getName.toString, ft.getAttribute(p.getName))
      }.toMap

    def attribute[D](name: String): D =
      ft.getAttribute(name).asInstanceOf[D]
  }

  def readSimpleFeatures(path: String) = {
    // Extract the features as GeoTools 'SimpleFeatures'
    val url = s"file://${new File(path).getAbsolutePath}"
    val ds = new ShapefileDataStore(new URL(url))
    val ftItr: SimpleFeatureIterator = ds.getFeatureSource.getFeatures.features

    try {
      val simpleFeatures = mutable.ListBuffer[SimpleFeature]()
      while(ftItr.hasNext) simpleFeatures += ftItr.next()
      simpleFeatures.toList
    } finally {
      ftItr.close
      ds.dispose
    }
  }

  def readPointFeatures(path: String): Seq[PointFeature[Map[String,Object]]] =
    readSimpleFeatures(path)
      .flatMap { ft => ft.geom[jts.Point].map(PointFeature(_, ft.attributeMap)) }

  def readPointFeatures[D](path: String, dataField: String): Seq[PointFeature[D]] =
    readSimpleFeatures(path)
      .flatMap { ft => ft.geom[jts.Point].map(PointFeature(_, ft.attribute[D](dataField))) }

  def readLineFeatures(path: String): Seq[LineFeature[Map[String,Object]]] =
    readSimpleFeatures(path)
      .flatMap { ft => ft.geom[jts.LineString].map(LineFeature(_, ft.attributeMap)) }

  def readLineFeatures[D](path: String, dataField: String): Seq[LineFeature[D]] =
    readSimpleFeatures(path)
      .flatMap { ft => ft.geom[jts.LineString].map(LineFeature(_, ft.attribute[D](dataField))) }

  def readPolygonFeatures(path: String): Seq[PolygonFeature[Map[String,Object]]] =
    readSimpleFeatures(path)
      .flatMap { ft => ft.geom[jts.Polygon].map(PolygonFeature(_, ft.attributeMap)) }

  def readPolygonFeatures[D](path: String, dataField: String): Seq[PolygonFeature[D]] =
    readSimpleFeatures(path)
      .flatMap { ft => ft.geom[jts.Polygon].map(PolygonFeature(_, ft.attribute[D](dataField))) }

  def readMultiPointFeatures(path: String): Seq[MultiPointFeature[Map[String,Object]]] =
    readSimpleFeatures(path)
      .flatMap { ft => ft.geom[jts.MultiPoint].map(MultiPointFeature(_, ft.attributeMap)) }

  def readMultiPointFeatures[D](path: String, dataField: String): Seq[MultiPointFeature[D]] =
    readSimpleFeatures(path)
      .flatMap { ft => ft.geom[jts.MultiPoint].map(MultiPointFeature(_, ft.attribute[D](dataField))) }

  def readMultiLineFeatures(path: String): Seq[MultiLineFeature[Map[String,Object]]] =
    readSimpleFeatures(path)
      .flatMap { ft => ft.geom[jts.MultiLineString].map(MultiLineFeature(_, ft.attributeMap)) }

  def readMultiLineFeatures[D](path: String, dataField: String): Seq[MultiLineFeature[D]] =
    readSimpleFeatures(path)
      .flatMap { ft => ft.geom[jts.MultiLineString].map(MultiLineFeature(_, ft.attribute[D](dataField))) }

  def readMultiPolygonFeatures(path: String): Seq[MultiPolygonFeature[Map[String,Object]]] =
    readSimpleFeatures(path)
      .flatMap { ft => ft.geom[jts.MultiPolygon].map(MultiPolygonFeature(_, ft.attributeMap)) }

  def readMultiPolygonFeatures[D](path: String, dataField: String): Seq[MultiPolygonFeature[D]] =
    readSimpleFeatures(path)
      .flatMap { ft => ft.geom[jts.MultiPolygon].map(MultiPolygonFeature(_, ft.attribute[D](dataField))) }
}

