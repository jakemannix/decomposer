package org.decomposer.util;

/**
 *
 * @author jmannix
 */
public class MathTestUtils 
{
  public static boolean isWithinEpsilon(double d1, double d2, double epsilon)
  {
    return Math.abs(d1 - d2) < epsilon;
  }
}
