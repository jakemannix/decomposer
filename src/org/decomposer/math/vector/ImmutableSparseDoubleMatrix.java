package org.decomposer.math.vector;

import java.util.Map;

import org.decomposer.math.vector.array.DenseMapVectorFactory;
import org.decomposer.math.vector.array.ImmutableSparseMapVector;


public class ImmutableSparseDoubleMatrix extends HashMapDoubleMatrix
{
  private static final long serialVersionUID = 1L;
  
  public ImmutableSparseDoubleMatrix(VectorFactory vectorFactory)
  {
    super(vectorFactory);
  }
  
  public ImmutableSparseDoubleMatrix()
  {
    super(new DenseMapVectorFactory()); 
  }
  
  public ImmutableSparseDoubleMatrix(DoubleMatrix other)
  {
    this();
    for(Map.Entry<Integer, MapVector> vectorEntry : other)
    {
      set(vectorEntry.getKey(), vectorEntry.getValue());
    }
  }

  @Override
  public DoubleMatrix scale(double scale)
  {
    throw new UnsupportedOperationException(getClass().getName() + " is immutable");
  }
  
  @Override
  public void set(int rowNumber, MapVector row)
  {
    if(get(rowNumber) != null)
    {
      throw new UnsupportedOperationException(getClass().getName() + " is immutable");
    }
    else
    {
      super.set(rowNumber, new ImmutableSparseMapVector(row));
    }
  }
}
