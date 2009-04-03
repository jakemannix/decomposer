package org.decomposer.math;

public class PrecisionUtils
{
  public static final double EPSILON = 10e-15;
  public static final boolean almostEquals(double d1, double d2)
  {
    return (d1 == 0 && d2 == 0) || Math.abs((d1/d2) - 1) < EPSILON;
  }
}
