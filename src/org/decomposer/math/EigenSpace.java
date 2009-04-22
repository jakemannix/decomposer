package org.decomposer.math;

import org.decomposer.math.vector.MapVector;

public interface EigenSpace
{
  /**
   * Take the input (sparse) document vector, and project onto the space spanned by the eigenvectors,
   * normalized by 1/singularValue.
   * @param documentVector
   * @return the projected vector, of dimension eigenVectors.size()
   */
  public MapVector project(MapVector documentVector);
  
  /**
   * Take an input (dense, but low-dimensional) projected vector, and (vector) sum the eigenvectors 
   * scaled by the projected vector's value at that eigen-index (again, suitably normalized by the 
   * singularValue).
   * @param projectedVector
   * @return dense vector of dimension eigenVectors.numCols()
   */
  public MapVector uplift(MapVector projectedVector);
  
  /**
   * 
   * @param input
   * @param numNonZeroEntries
   * @return a nearby sparse vector to the input with only numNonZeroEntries non-zero entries
   */
  public MapVector sparsify(MapVector input, final int numNonZeroEntries);
  
  /**
   * essentially sparsify(uplift(project(documentVector)), numOutPutNumZeroEntries)
   * @param documentVector
   * @param numOutpuNonZeroEntries
   * @return a sparsified uplift of the eigen-projection of the input document vector.
   */
  public MapVector expand(MapVector documentVector, int numOutpuNonZeroEntries);

}
