package org.decomposer.math;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.decomposer.math.vector.DoubleMatrix;
import org.decomposer.math.vector.MapVector;


public class MultiThreadedEigenVerifier extends SimpleEigenVerifier
{
  protected final Executor _threadPool;
  protected EigenStatus _status = null;
  protected boolean _finished = false;
  protected boolean _started = false;
  
  public MultiThreadedEigenVerifier()
  {
    _threadPool = Executors.newFixedThreadPool(1);
    _status = new EigenStatus(-1, 0);
  }
  
  @Override
  public EigenStatus verify(DoubleMatrix eigenMatrix, MapVector vector)
  {
    synchronized(_status)
    {
      if(!_finished && !_started) // not yet started or finished, so start!
      {
        _status = new EigenStatus(-1, 0);
        MapVector vectorCopy = vector.clone();
        _threadPool.execute(new VerifierRunnable(eigenMatrix, vectorCopy));
        _started = true;
      }
      if(_finished) _finished = false;
      return _status;
    }
  }
  
  protected EigenStatus innerVerify(DoubleMatrix eigenMatrix, MapVector vector)
  {
    return super.verify(eigenMatrix, vector);
  }
  
  protected class VerifierRunnable implements Runnable
  {
    DoubleMatrix _eigenMatrix;
    MapVector _vector;
    public VerifierRunnable(DoubleMatrix eigenMatrix, MapVector vector)
    {
      _eigenMatrix = eigenMatrix;
      _vector = vector;
    }
    public void run()
    {
      EigenStatus status = innerVerify(_eigenMatrix, _vector);
      synchronized(_status)
      {
        _status = status;
        _finished = true;
        _started = false;
      }
    } 
  }
}
