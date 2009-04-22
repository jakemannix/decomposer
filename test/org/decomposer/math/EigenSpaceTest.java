package org.decomposer.math;

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
    HebbianSolverTest.timeSolver(state);
    EigenSpace eigenSpace = new EigenSpaceImpl(state.currentEigens,
                                               state.currentEigenValues.toArray(new Double[0]));
    MapVector documentVector;
  }

}
