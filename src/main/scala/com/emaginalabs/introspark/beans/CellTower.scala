package com.emaginalabs.introspark.beans

/**
 * Created by Francisco de Atilano
 */
case class CellTower(val radio: String,
                     val mcc: String,
                     val net: String,
                     val area: String,
                     val cell: String,
                     val unit: String,
                     val lon: String,
                     val lat: String,
                     val range: String,
                     val samples: String,
                     val changeable: String,
                     val created: String,
                     val updated: String,
                     val averageSignal: String) {

  override def toString = radio + "," + mcc + "," + net + "," + area + "," + cell + "," + unit + "," + lon + "," + lat + "," + range + "," + samples + "," + changeable + "," + created + "," + updated + "," + averageSignal
}
