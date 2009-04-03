package org.decomposer.math.vector;

import java.io.Serializable;
import java.util.Map;

public interface DoubleMatrix extends Iterable<Map.Entry<Integer, MapVector>>, Serializable
{
  MapVector times(MapVector vector);
  MapVector timesSquared(MapVector vector);
  DoubleMatrix scale(double scale);
  MapVector get(int rowNumber);
  void set(int rowNumber, MapVector row);
  int numRows();
  int numCols();
}
