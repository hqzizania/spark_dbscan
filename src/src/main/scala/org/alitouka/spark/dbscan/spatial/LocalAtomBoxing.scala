package org.alitouka.spark.dbscan.spatial

import org.alitouka.spark.dbscan.{DbscanSettings, RawDataSet}
import org.apache.spark.rdd.RDD

/**
  * Created by qhuang on 01/06/2017.
  */
class LocalAtomBoxing {

}

object LocalAtomBoxing {

  def apply(data: RawDataSet, settings: DbscanSettings): (RawDataSet, RawDataSet) = {
    val boxCalculator = new BoxCalculator(data)
    val bounding = boxCalculator.calculateBoundingBox.bounds
    val atomBoxingSlide = settings.epsilon / math.sqrt(boxCalculator.getNumberOfDimensions(data))
    val dataWithAtomId: RDD[(String, Point)] = data.map { point =>
      val ids = point.coordinates.zipWithIndex.map { case (x, index) =>
        ((x - bounding(index).lower) / atomBoxingSlide).toInt
      }
      var s: String = ""
      ids.foreach(x => s = s + x.toString + "x")
      (s, point.withAtomBoxId(s))
    }

    (dataWithAtomId.map(_._2), dataWithAtomId.groupBy(_._1).map{case(k, v) => v.head._2})
  }
}
