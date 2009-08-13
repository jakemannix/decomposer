package org.decomposer.math.vector;

public class IntDoublePairImpl implements IntDoublePair
{
  private final int _i;
  private double _d;
  public IntDoublePairImpl(IntDoublePair pair)
  {
    this(pair.getInt(), pair.getDouble());
  }
  public IntDoublePairImpl(int i, double d)
  {
    _i = i;
    _d = d;
  }
  public double getDouble()
  {
    return _d;
  }

  public int getInt()
  {
    return _i;
  }

  public void setDouble(double newValue)
  {
    _d = newValue;
  }

}
