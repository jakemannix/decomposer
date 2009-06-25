package org.decomposer.contrib.lsh;

import junit.framework.TestCase;
import org.decomposer.math.vector.MapVector;
import org.decomposer.math.vector.array.DenseMapVectorFactory;

import static org.decomposer.contrib.lsh.LocalitySensitiveHashIndexImpl.*;
/**
 *
 * @author jmannix
 */
public class LocalitySensitiveHashImplTest extends TestCase
{

  public LocalitySensitiveHashImplTest(String testName)
  {
    super(testName);
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
  
  public void testBitSwap() throws Exception
  {
    long one = 1L;
    long sixteen = Permutation.swap(one, 0, 4);
    assertEquals(16L, sixteen);
    for(int i=0; i<30; i++)
    {
      for(int j=i; j<30; j++)
      {
        for(int n=1; n<1000; n++)
        {
          assertEquals(n, Permutation.swap(Permutation.swap(n, i, j), i, j));
        }
      }
    }
  }
    
  int numBits = 16;
  int numPermutations = 16;
  int sourceDimension = 10000;
  boolean sparse = false;
    
  public void testLSHNumSetBits() throws Exception
  {
    LocalitySensitiveHash lsh = new LocalitySensitiveHashImpl(numBits, numPermutations, sourceDimension, sparse);
    double meanSetBits = 0;
    for(int i=0; i<100; i++)
    {
      MapVector v = LocalitySensitiveHashIndexImplTest.randomVector(new DenseMapVectorFactory(), 
                                                                    sourceDimension, 
                                                                    sourceDimension);
      long[] hashes = lsh.hash(v);
      meanSetBits += LocalitySensitiveHashIndexImpl.hammingDistance(hashes[0], 0L);
    }
    meanSetBits /= 100;
    System.out.println("Mean set bits: " + meanSetBits);
    assertTrue(Math.abs(meanSetBits - numBits/2) < 0.5);
  }
  
  public void testHashEqualityRadius() throws Exception
  {
    numBits = 64;
    LocalitySensitiveHash lsh = new LocalitySensitiveHashImpl(numBits, numPermutations, sourceDimension, sparse);
    MapVector v = LocalitySensitiveHashIndexImplTest.randomVector(new DenseMapVectorFactory(),
                                                                  sourceDimension,
                                                                  sourceDimension);
    v.scale(1/v.norm());
      
    long[] vHashes = lsh.hash(v);
      
    for(int i=0; i<1000; i+=10)
    {
      double dist = 0;
      double meanCosTheta = 0;
      for(int num = 0; num < 10; num++)
      {
        MapVector w = LocalitySensitiveHashIndexImplTest.randomVector(new DenseMapVectorFactory(),
                                                                      sourceDimension,
                                                                      sourceDimension);
        w.scale((i * 0.01)/w.norm());
        w = w.plus(v);
        long[] wHashes = lsh.hash(w);
        int hammingDistance = Integer.MAX_VALUE;
        for(int j=0; j<vHashes.length; j++)
          hammingDistance = (int) Math.min(hammingDistance, hammingDistance(vHashes[j], wHashes[j]));
        dist += hammingDistance;
        meanCosTheta += w.dot(v) / (w.norm() * v.norm());
      }
      System.out.println("1-(cos(theta)) = " + (1-meanCosTheta/10) + ", HammingDistance = " + (dist/10));
    }
  }

  public void testPermutation() throws Exception
  {
    Permutation p = new Permutation(new int[]
                                    {
                                      5, 6, 4, 1
                                    });
    for(byte b=1; b<127; b++)
    {
      //System.out.println("byte: " + b);
      //System.out.println(print(b));
      byte b1 = (byte) p.perm(b);
      //System.out.println(print(b1));
      byte b2 = (byte) p.perm(b1);
      //System.out.println(print(b2));
      //System.out.println("");
      assertEquals(b2, b);
    }
  }

  public static String print(byte b)
  {
    int mask = 1;
    char[] bits = new char[8];
    for (int i = 0; i < 8; i++)
    {
      int bit = (b & (mask << i));
      bits[7-i] = (bit != 0) ? '1' : '0';
    }
    return new String(bits);
  }
}
