package org.decomposer.contrib.lsh;

import java.util.Random;
import org.decomposer.math.vector.MapVector;
import org.decomposer.math.vector.VectorFactory;
import org.decomposer.math.vector.array.DenseMapVectorFactory;
import org.decomposer.math.vector.array.ImmutableSparseMapVector;
import org.decomposer.math.vector.hashmap.HashMapVectorFactory;

/**
 *
 * @author jmannix
 */
public class LocalitySensitiveHashImpl implements LocalitySensitiveHash
{
  protected final int _numBits;
  protected final int _numPermutations;
  protected final MapVector[] _projectors;
  protected final Permutation[] _permutations;
 
  public LocalitySensitiveHashImpl(int numBits, int numPermutations, int sourceDimension, boolean sparse)
  {
    this(numBits, numPermutations, sourceDimension, sparse, System.currentTimeMillis());
  }
  
  public LocalitySensitiveHashImpl(int numBits, int numPermutations, int sourceDimension, boolean sparse, long seed)
  {
    _numBits = numBits;
    _numPermutations = numPermutations;
    Random rand = new Random(seed);
    _permutations = new Permutation[_numPermutations];
    for(int i=0; i<_permutations.length; i++)
    {
      _permutations[i] = new Permutation(_numBits, rand.nextInt() + i);
    }
    _projectors = new MapVector[_numBits];
    for(int i=0; i<_projectors.length; i++)
    {
      _projectors[i] = randomProjector(sparse ? numBits : sourceDimension, sourceDimension, rand.nextInt());
    }
  }

  public long[] hash(MapVector input)
  {
    final long hashSingle = hashSingle(input);
    long[] hashes = new long[_numPermutations];
    for(int i=0; i<hashes.length; i++) hashes[i] = _permutations[i].perm(hashSingle);
    return hashes;
  }

  public long hashSingle(MapVector input)
  {
    long hash = 0;
    for(int i=0; i<_projectors.length; i++)
    {
      if(_projectors[i].dot(input) > 0) hash |= (1L << i);
    }
    return hash;
  }
  
  public MapVector randomProjector(int numNonZeroBits, int maxDimension, long seed)
  {
    Random rand = new Random(seed);
    VectorFactory vf = (numNonZeroBits < maxDimension) ? new HashMapVectorFactory() : new DenseMapVectorFactory();
    MapVector v = vf.zeroVector(numNonZeroBits);
    for(int i=0; i<numNonZeroBits; i++) 
    {
      int index = (numNonZeroBits < maxDimension) ? rand.nextInt(maxDimension) : i;
      v.set(index, rand.nextGaussian());
    }
    v.scale(1/v.norm());
    return (numNonZeroBits < maxDimension) ? new ImmutableSparseMapVector(v) : v;
  }
}
