package org.decomposer.math.vector;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Iterator;

/**
 * Basic interface for mathematical vectors (elements of a finite dimensional linear vector space).
 * Effectively extends Map<Integer, Double>, but is not really a true member of the java.util.collections api.
 * Different implementations may be tuned toward sparsity, immutability, or maximum flexibility.
 * 
 * @author jmannix
 *
 */
public interface MapVector extends Iterable<IntDoublePair>, Cloneable
{
  /**
   * get() is not guaranteed to run in constant time (sparse implementations may run in log(numNonZeroEntries) time), and if
   * access to all elements in turn is required, use the iterator.
   * @param index
   * @return the vector value at this position - returns zero if not set.
   */
  double get(int index);
  
  /**
   * random-access set() is also not guaranteed to run in constant time.  Dense implementations may run in amortized log(numEntries)
   * @param index
   * @param value
   */
  void set(int index, double value);
  
  /**
   * Same caveats apply as to set().  Implements effectively as set(index, get(index) + toBeAdded);
   * @param index
   * @param toBeAdded
   */
  void add(int index, double toBeAdded);
  
  /**
   * The number of nonZeroEntries is necessarily less than or equal to this number.  This in actuality returns the number of *set* entries.
   * @return the number of nonZeroEntries
   */
  int numNonZeroEntries();
  
  /**
   * The highest dimension with a nonzero entry currently in this vector
   * @return maxDimension
   */
  int maxDimension();
  
  /**
   * Should be assumed to be cached - only one call should perform any computation.
   * @return Math.sqrt(sum_i=0^HighestNonZeroIndex(Math.pow(get(i), 2));
   */
  double norm();
  
  /**
   * In case you need to save the Math.sqrt() call.
   * @return this.dot(this), effectively
   */
  double normSquared();
  
  /**
   * Mutates the vector by scalar multiplication.
   * @param scale
   * @return this, for ease of method chaining.
   */
  MapVector scale(double scale);
  
  /**
   * Mutates the vector by adding another one to it.
   * @param vector
   * @return this, for ease of method chaining
   */
  MapVector plus(MapVector vector);
  
  /**
   * Convenience method, effectively doing plus(vector.scale(scale)), 
   * but without mutating <code>vector</code>, and with less algorithmic complexity
   * @param vector
   * @param scale
   * @return this, for ease of method chaining
   */
  MapVector plus(MapVector vector, double scale);
  
  /**
   * Implementations should be smart enough to try and iterate over the smaller vector if 
   * random access in the larger is efficient.  Both vectors are access read-only.
   * @param vector
   * @return the vector dot product of this and <code>vector</code>
   */
  double dot(MapVector vector);
  
  /**
   * Should be a deep copy if read-write, but if read-only, returning this is allowed.
   * @return a deep copy or this.
   */
  MapVector clone();
  
  /**
   * No order is guaranteed for the iteration, nor is it required to be deterministic.
   */
  Iterator<IntDoublePair> iterator();
  
  public interface Formatter
  {
    public String toString(MapVector vector, int numEntriesToShow);
    public String toString(MapVector vector);
  }
  
  public static final Formatter F = new Formatter()
  {
    private NumberFormat f = new DecimalFormat("0.###E0");
    @Override
    public String toString(MapVector vector, int numEntriesToShow)
    {
      String s = "{ ";
      Iterator<IntDoublePair> it = vector.iterator();
      IntDoublePair pair = null;
      int i=0; 
      while(it.hasNext() && i<numEntriesToShow)
      {
        pair = it.next();
        i++;
        s += "" + pair.getInt() + ":" + f.format(pair.getDouble());
        if(it.hasNext()) s += ", ";
      }
      if(it.hasNext())
      {
        pair = it.next();
        s += "" + pair.getInt() + ":" + f.format(pair.getDouble());
      }
      if(it.hasNext()) { s += ", ..."; }
      s += " }";
      return s; 
    }
    public String toString(MapVector vector)
    {
      return toString(vector, 100);
    }
  };
}
