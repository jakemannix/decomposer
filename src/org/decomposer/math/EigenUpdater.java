package org.decomposer.math;

import org.decomposer.math.vector.MapVector;


public interface EigenUpdater
{
  void update(MapVector pseudoEigen, MapVector trainingVector, TrainingState currentState);
}
