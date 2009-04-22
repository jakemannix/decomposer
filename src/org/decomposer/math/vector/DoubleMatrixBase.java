package org.decomposer.math.vector;

import java.io.Serializable;
import java.util.Map;

import org.decomposer.math.vector.array.DenseMapVectorFactory;

/**
 * @author jmannix
 *
 */
public abstract class DoubleMatrixBase implements DoubleMatrix, Serializable
{
  private static final long serialVersionUID = 1L;
  protected transient VectorFactory _denseMapVectorFactory = new DenseMapVectorFactory();
  
  /**
   * 
   */
  public DoubleMatrix scale(double scale)
  {
    for(Map.Entry<Integer, MapVector> entry : this)
      entry.getValue().scale(scale);
    return this;
  }

  /**
   * 
   */
  public MapVector times(MapVector vector)
  {
    MapVector result = _denseMapVectorFactory.zeroVector(numRows());
    for(Map.Entry<Integer, MapVector> entry : this) 
      result.set(entry.getKey(), vector.dot(entry.getValue()));
    return result;
  }

  /**
   * 
   */
  public MapVector timesSquared(MapVector vector)
  {
    MapVector result = _denseMapVectorFactory.zeroVector(numCols());
    for(Map.Entry<Integer, MapVector> entry : this) 
      result.plus(entry.getValue(), entry.getValue().dot(vector));
    return result;
  }

}
