package org.decomposer.contrib.lsh;

import java.util.Random;

public final class Permutation
{
  private static final long[] masks = new long[64];
  static
  {
    for (int i = 0; i < 64; i++) masks[i] = (1L << i);
  }
  
  private final int[] _permutation;

  public Permutation(int numBits, int randomSeed)
  {
    _permutation = new int[numBits];
    Random rand = new Random(randomSeed);
    for (int i = 0; i < _permutation.length;
      i++)
    {
      _permutation[i] = rand.nextInt(numBits - i);
    }
  }

  public Permutation(int[] permutation)
  {
    _permutation = permutation;
  }

  public final long perm(final long l)
  {
    return perm(l, _permutation);
  }

  private final long perm(final long l, final int[] permOffsets)
  {
    long permutedLong = l;
    for (int i = 0; i < permOffsets.length; i++)
    {
      permutedLong = swap(permutedLong, i, i + permOffsets[i]);
    }
    return permutedLong;
  }

  public static void shuffle(int[] array, int randomSeed)
  {
    Random rng = new Random(randomSeed);
    int n = array.length;
    while (n > 1)
    {
      int k = rng.nextInt(n);
      n--;
      int temp = array[n];
      array[n] = array[k];
      array[k] = temp;
    }
  }

  /**
   * complexity: one method call, two long subtractions, four array accesses, and nine bit operations
   * @param b input bitsequence to swap the bits of
   * @param i first index (measured from the lowest bit: right) - must be <= j
   * @param j second index - must be >= i
   * @return the swapped bitsequence
   */
  public static final long swap(final long b, final int i, final int j)
  {
    return (b & ~((1L << i) | (1L << j))) | ((b & (1L << i)) << (j - i)) | ((b & (1L << j)) >> (j - i));
  }
}
