package org.decomposer.math.vector.hashmap;

import org.decomposer.math.vector.IntDoublePair;
import org.decomposer.math.vector.MapVector;
import org.decomposer.math.vector.array.ImmutableSparseMapVector;
import org.decomposer.math.vector.hashmap.HashMapVector;

import junit.framework.TestCase;

public class TestMapVector extends TestCase
{

  public TestMapVector(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    super.setUp();
  }

  protected void tearDown() throws Exception
  {
    super.tearDown();
  }

  public void testDot()
  {
    MapVector v1 = randomVector(100, 1000, 1.0);
    MapVector v2 = randomVector(500, 2000, 1.0);
    assertDotEquals(v1, v2);
    v1 = randomImmutableSparseVector(50, 100, 1.0);
    v2 = randomImmutableSparseVector(55, 110, 1.0);
    assertDotEquals(v1, v2);
  }
  
  private void assertDotEquals(MapVector v1, MapVector v2)
  {
    double dot = 0;
    for(IntDoublePair pair : v1)
    {
      dot += pair.getDouble() * v2.get(pair.getInt());
    }
    double calc = v1.dot(v2);
    assertWithinEpsilon(dot, calc);
  }

  public void testNorm()
  {
    MapVector v = randomVector(100, 1000, 10.0);
    assertNormEquals(v);
    v = randomImmutableSparseVector(100, 1000, 10.0);
    assertNormEquals(v);
  }
  
  private void assertNormEquals(MapVector v)
  {
    double norm = 0;
    for(IntDoublePair pair : v)
    {
      norm += pair.getDouble() * pair.getDouble();
    }
    assertWithinEpsilon(Math.sqrt(norm), v.norm());
  }

  public void testPlus()
  {
    MapVector v1 = randomVector(100, 1000, 1.0);
    MapVector v2 = randomVector(200, 10000, 0.01);
    MapVector sum = new HashMapVector();
    for(int i=0; i<10000; i++)
    {
      sum.set(i, v1.get(i) + v2.get(i));
    }
    v2.plus(v1);
    assertTrue(v2.equals(sum));
  }

  public void testScale()
  {
    MapVector v = randomVector(100, 1000, 1.0);
    double norm = v.norm();
    assertWithinEpsilon(norm*123.45678, v.scale(123.45678).norm());
  }
  
  public void testEquals()
  {
    MapVector v = randomVector(100, 1000, 1.0);
    MapVector w = v.clone();
    assertTrue(v.equals(w));
    w = new ImmutableSparseMapVector(v);
    assertTrue(v.equals(w));
  }
  
  private void assertWithinEpsilon(double d1, double d2)
  {
    assertWithinEpsilon(d1, d2, 10e-12);
  }
  
  private void assertWithinEpsilon(double d1, double d2, double epsilon)
  {
    assertTrue("" + d1 + " - " + d2 + " = " + (d1-d2), Math.abs(d1 - d2) < epsilon);
  }
  
  protected MapVector randomVector(int numNonZero, int maxIndex, double absMeanValue)
  {
    MapVector vector = new HashMapVector();
    for(int i=0; i<numNonZero; i++)
    {
      int index = (int)(Math.random()*maxIndex);
      double value = (Math.random()*2*absMeanValue*(Math.random() > 0.5 ? 1.0 : -1.0));
      vector.set(index, value);
    }
    return vector;
  }
  
  protected MapVector randomImmutableSparseVector(int numNonZero, int maxIndex, double absMeanValue)
  {
    return new ImmutableSparseMapVector(randomVector(numNonZero, maxIndex, absMeanValue));
  }

}
