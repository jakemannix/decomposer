package org.decomposer.math.vector.hashmap;

import org.decomposer.math.vector.MapVector;
import org.decomposer.math.vector.VectorFactory;

public class HashMapVectorFactory implements VectorFactory
{
  public MapVector zeroVector()
  {
    return new HashMapVector();
  }
  
  public MapVector zeroVector(int initialSize)
  {
    return new HashMapVector(initialSize);
  }
}
