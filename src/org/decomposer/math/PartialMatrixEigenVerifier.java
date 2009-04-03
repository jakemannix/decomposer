package org.decomposer.math;

import org.decomposer.math.vector.DoubleMatrix;
import org.decomposer.math.vector.ImmutableSparseDoubleMatrix;
import org.decomposer.math.vector.MapVector;

public class PartialMatrixEigenVerifier extends MultiThreadedEigenVerifier
{

  protected double _subMatrixFraction;
  protected int _maxSubMatrixRows;
  protected int _minSubMatrixRows;
  
  public PartialMatrixEigenVerifier(double subMatrixFraction, int maxSubMatrixRows, int minSubMatrixRows)
  {
    _subMatrixFraction = subMatrixFraction;
    _maxSubMatrixRows = maxSubMatrixRows;
    _minSubMatrixRows = minSubMatrixRows;
  }
  
  /**
   * <code>innerVerify</code> is called from another thread in <code>MultiThreadedEigenVerifier</code>
   */
  @Override
  protected EigenStatus innerVerify(DoubleMatrix eigenMatrix, MapVector vector)
  {
    DoubleMatrix subMatrix = new ImmutableSparseDoubleMatrix();
    int numRows = numRows(eigenMatrix.numRows());
    while(subMatrix.numRows() < numRows)
    {
      int index = (int)(Math.random() * eigenMatrix.numRows());
      if(subMatrix.get(index) == null)
        subMatrix.set(index, eigenMatrix.get(index));
    }
    return super.innerVerify(subMatrix, vector);
  }

  private int numRows(int numTotalRows)
  {
    int fractionRows = (int)(_subMatrixFraction * numTotalRows);
    if(fractionRows > _maxSubMatrixRows)
    {
      return _maxSubMatrixRows;
    }
    if(fractionRows < _minSubMatrixRows)
    {
      return Math.min(_minSubMatrixRows, numTotalRows);
    }
    return fractionRows;
  }
  
  
}
