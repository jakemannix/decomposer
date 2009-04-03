package org.decomposer.math;

import java.util.logging.Logger;

import org.decomposer.math.vector.MapVector;
import org.decomposer.math.vector.VectorFactory;


public class HebbianUpdater implements EigenUpdater
{
  private static final Logger log = Logger.getLogger(HebbianUpdater.class.getName());
  
  public void update(MapVector pseudoEigen,
                     MapVector trainingVector,
                     TrainingState currentState)
  {
    double trainingVectorNorm = trainingVector.norm();
    int numPreviousEigens = currentState.currentEigens.numRows();
    if(numPreviousEigens > 0)
    {
      if(currentState.firstPass)
      {
        updateTrainingProjectionsVector(currentState,
                                        trainingVector,
                                        numPreviousEigens - 1);
      }      
    }
    if(currentState.activationDenominatorSquared == 0 || trainingVectorNorm == 0)
    {
      if(currentState.activationDenominatorSquared == 0) 
      {
        pseudoEigen.plus(trainingVector);
        currentState.helperVector = currentState.currentTrainingProjection().clone();
        currentState.activationDenominatorSquared = trainingVectorNorm * trainingVectorNorm - currentState.helperVector.normSquared();
      }
      return;
    }
    currentState.activationNumerator = pseudoEigen.dot(trainingVector);
    currentState.activationNumerator -= currentState.helperVector.dot(currentState.currentTrainingProjection());
    
    double activation = currentState.activationNumerator / Math.sqrt(currentState.activationDenominatorSquared);
    currentState.activationDenominatorSquared += 2 * activation * currentState.activationNumerator 
                                              + (activation*activation)*(trainingVector.normSquared() - currentState.currentTrainingProjection().normSquared());
    if(numPreviousEigens > 0)
      currentState.helperVector.plus(currentState.currentTrainingProjection(), activation);
    pseudoEigen.plus(trainingVector, activation);
  }

  private void updateTrainingProjectionsVector(TrainingState state,
                                               MapVector trainingVector,
                                               int previousEigenIndex)
  {
    MapVector previousEigen = state.mostRecentEigen();
    MapVector currentTrainingVectorProjection = state.currentTrainingProjection();
    currentTrainingVectorProjection.set(previousEigenIndex, 
                                        previousEigen.dot(trainingVector));
  }
  
}
