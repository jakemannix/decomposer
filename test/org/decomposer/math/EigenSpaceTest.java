package org.decomposer.math;

import java.util.Map.Entry;

import org.decomposer.math.vector.DoubleMatrix;
import org.decomposer.math.vector.MapVector;

import junit.framework.TestCase;

public class EigenSpaceTest extends TestCase
{

  public EigenSpaceTest(String name)
  {
    super(name);
  }

  @Override
  protected void setUp() throws Exception
  {
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception
  {
    super.tearDown();
  }
  
  public void testEigenSpace() throws Exception
  {
    TrainingState state = new TrainingState();
    HebbianSolverTest.timeSolver(state, 500);
    EigenSpace eigenSpace = new EigenSpaceImpl(state.currentEigens,
                                               state.currentEigenValues.toArray(new Double[0]));
    DoubleMatrix corpus = HebbianSolverTest._previousCorpus;
    int i=0;
    for(Entry<Integer, MapVector> entry : corpus)
    {
      MapVector vector = entry.getValue();
      MapVector projected = eigenSpace.sparsify(eigenSpace.uplift(eigenSpace.project(vector)), 100);
      assertTrue(projected.norm() < vector.norm());
      int index = entry.getKey();
      System.out.println("cosAngle = " + vector.dot(projected)/(vector.norm()*projected.norm()));
      System.out.println("Original Vector [" + index + "]: "+ vector);
      System.out.println("Expanded Vector [" + index + "]: "+ projected + "\n");
      if(i++ > 100) break;
    }
  }

}
