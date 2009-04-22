package org.decomposer.math;

import java.util.ArrayList;
import java.util.List;

import org.decomposer.math.vector.DoubleMatrix;
import org.decomposer.math.vector.HashMapDoubleMatrix;
import org.decomposer.math.vector.MapVector;
import org.decomposer.math.vector.VectorFactory;
import org.decomposer.math.vector.array.DenseMapVectorFactory;


public class TrainingState
{
  TrainingState()
  {
    this(new DenseMapVectorFactory(), 
         new HashMapDoubleMatrix(new DenseMapVectorFactory()), 
         new HashMapDoubleMatrix(new DenseMapVectorFactory()));
  }
  TrainingState(VectorFactory factory, DoubleMatrix eigens, DoubleMatrix projections)
  {
    trainingProjectionFactory = factory;
    currentEigens = eigens;
    trainingProjections = projections;
    trainingIndex = 0;
    helperVector = factory.zeroVector();
    firstPass = true;
    statusProgress = new ArrayList<EigenStatus>();
    activationNumerator = 0;
    activationDenominatorSquared = 0;
  }
  
  VectorFactory trainingProjectionFactory;
  public DoubleMatrix currentEigens;
  public List<Double> currentEigenValues;
  DoubleMatrix trainingProjections;
  int trainingIndex;
  MapVector helperVector;
  boolean firstPass;
  List<EigenStatus> statusProgress;
  double activationNumerator;
  double activationDenominatorSquared;
  
  public MapVector mostRecentEigen()
  {
    return currentEigens.get(currentEigens.numRows() - 1);
  }
  
  public MapVector currentTrainingProjection()
  {
    if(trainingProjections.get(trainingIndex) == null)
      trainingProjections.set(trainingIndex, trainingProjectionFactory.zeroVector());
    return trainingProjections.get(trainingIndex);
  }
}
