package org.decomposer.math;

public class EigenStatus
{
  private final double _eigenValue;
  private final double _cosAngle;
  
  public EigenStatus(double eigenValue, double cosAngle)
  {
    _eigenValue = eigenValue;
    _cosAngle = cosAngle;
  }

  public double getCosAngle()
  {
    return _cosAngle;
  }

  public double getEigenValue()
  {
    return _eigenValue;
  }
}
