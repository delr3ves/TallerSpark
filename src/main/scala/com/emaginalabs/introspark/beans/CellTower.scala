package com.emaginalabs.introspark.beans

/**
 * Created by Francisco de Atilano
 */
case class CellTower(radio: String,
                     mcc: String,
                     net: String,
                     area: String,
                     cell: String,
                     unit: String,
                     lon: String,
                     lat: String,
                     range: String,
                     samples: String,
                     changeable: String,
                     created: String,
                     updated: String,
                     averageSignal: String) {

  override def toString = radio + "," + mcc + "," + net + "," + area + "," + cell + "," + unit + "," + lon + "," + lat + "," + range + "," + samples + "," + changeable + "," + created + "," + updated + "," + averageSignal
}
