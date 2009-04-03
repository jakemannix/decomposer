package org.decomposer.math;

import java.util.logging.Logger;

import org.decomposer.math.vector.DoubleMatrix;
import org.decomposer.math.vector.MapVector;


public class SimpleEigenVerifier implements SingularVectorVerifier
{
  private static final Logger log = Logger.getLogger(SimpleEigenVerifier.class.getName());
  
  public EigenStatus verify(DoubleMatrix eigenMatrix, MapVector vector)
  {
    MapVector resultantVector = eigenMatrix.timesSquared(vector);
    double newNorm = resultantVector.norm();
    double oldNorm = vector.norm();
    double eigenValue = (newNorm > 0 && oldNorm > 0) ? newNorm / oldNorm : 1; 
    double cosAngle = (newNorm > 0 && oldNorm > 0) ? resultantVector.dot(vector) / (newNorm * oldNorm) : 0;
    return new EigenStatus(eigenValue, cosAngle);
  }

}
