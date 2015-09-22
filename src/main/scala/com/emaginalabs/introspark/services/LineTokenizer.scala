package com.emaginalabs.introspark.services

/**
 * @author Sergio Arroyo - @delr3ves
 */
class LineTokenizer {
  def tokenize(line: String): Array[String] = {
    line.split( Array(' ', ',', '.'))
  }
}
