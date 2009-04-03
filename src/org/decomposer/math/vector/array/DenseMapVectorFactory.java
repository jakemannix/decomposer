package org.decomposer.math.vector.array;

import org.decomposer.math.vector.MapVector;
import org.decomposer.math.vector.VectorFactory;

public class DenseMapVectorFactory implements VectorFactory
{

  public MapVector zeroVector()
  {
    return new DenseMapVector();
  }

  public MapVector zeroVector(int initialSize) 
  {
    return new DenseMapVector(initialSize);
  }

}
