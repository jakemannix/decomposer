package org.decomposer.math;

import org.decomposer.math.vector.DoubleMatrix;
import org.decomposer.math.vector.MapVector;

public interface SingularVectorVerifier
{
  EigenStatus verify(DoubleMatrix eigenMatrix, MapVector vector);
}
