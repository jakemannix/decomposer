package org.decomposer.contrib.lsh;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.decomposer.math.vector.MapVector;

/**
 *
 * @author jmannix
 */
public class LocalitySensitiveHashIndexImpl implements LocalitySensitiveHashIndex
{
  protected final LocalitySensitiveHash _lsh;
  protected transient List<List<LSHNode>> _nodes;
  protected final long[][] _keys;
  protected final MapVector[][][] _values;
  protected int _defaultNumResults;
  protected boolean _sealed;

  public LocalitySensitiveHashIndexImpl(LocalitySensitiveHash lsh, 
                                        int numPermutations, 
                                        int defaultNumResults)
  {
    _lsh = lsh;
    _nodes = new ArrayList<List<LSHNode>>(numPermutations);
    for(int i=0; i<numPermutations; i++) _nodes.add(new ArrayList<LSHNode>());
    _defaultNumResults = defaultNumResults;
    _keys = new long[numPermutations][];
    _values = new MapVector[numPermutations][][];
    _sealed = false;
  }
  
  public void add(MapVector docVector)
  {
    if(_sealed) throw new IllegalStateException("LSHIndex has been sealed, no adds accepted anymore.");
    long[] hashes = _lsh.hash(docVector);
    for(int i=0; i<_nodes.size(); i++)
    {
      LSHNode node = new LSHNode(hashes[i], docVector);
      _nodes.get(i).add(node);
    }
  }
  
  public void sortAndSeal()
  {
    for(int i=0; i<_nodes.size(); i++)
    {
      List<LSHNode> nodes = _nodes.get(i);
      Collections.sort(nodes);
      LSHNode[] sortedNodes = trimEmpty(nodes);
      _keys[i] = new long[sortedNodes.length];
      _values[i] = new MapVector[sortedNodes.length][];
      for(int j=0; j<sortedNodes.length; j++)
      {
        _keys[i][j] = sortedNodes[j]._key;
        Set<MapVector> vectors = sortedNodes[j]._values;
        _values[i][j] = new MapVector[vectors.size()];
        int k=0;
        for(MapVector v : vectors)
        {
          _values[i][j][k] = v;
          k++;
        }
      }
    }
    _nodes.clear();
    _sealed = true;
  }

  public List<MapVector> findNearest(MapVector queryVector)
  {
    return findNearest(queryVector, _defaultNumResults);
  }

  public List<MapVector> findNearest(MapVector queryVector, int numResults)
  {
    if(!_sealed) throw new IllegalStateException("Cannot search until seal() has been called.");
    long[] hashes = _lsh.hash(queryVector);
    Map<Long, List<MapVector>> scoredVectors = new TreeMap<Long, List<MapVector>>();
    for(int i=0; i<_keys.length; i++)
    {
      int index = Arrays.binarySearch(_keys[i], hashes[i]);
      int numFoundSoFar = 0;
      int valueLength = _values[i].length;
      if(index < 0)
      {
        index = -(index + 1);
        if(index == valueLength) index--;
      }
      numFoundSoFar += add(scoredVectors, hashes, i, index);
      int offset = 1;
      while(numFoundSoFar < numResults)
      {
        if(index + offset < valueLength) numFoundSoFar += add(scoredVectors, hashes, i, index + offset);
        if(index - offset >= 0) numFoundSoFar += add(scoredVectors, hashes, i, index - offset);
        if(index + offset > valueLength && index - offset < 0) 
        {
          // throw exception?  returning whole bucket...  probably a warning would be nice.
          break;
        }
        offset++;
      }
    }
    List<MapVector> nearest = new ArrayList<MapVector>(numResults);
    Iterator<Map.Entry<Long, List<MapVector>>> scoredVectorIterator = scoredVectors.entrySet().iterator();
    boolean foundAllExactMatches = false;
    while(scoredVectorIterator.hasNext())
    {
      if(foundAllExactMatches && nearest.size() > numResults) break;
      Map.Entry<Long, List<MapVector>> e = scoredVectorIterator.next();
      if(e.getKey() != 0) foundAllExactMatches = true;
      nearest.addAll(e.getValue());
    }
    return nearest;
  }
  
  private int add(Map<Long, List<MapVector>> scoredVectors, long[] hashes, int i, int index)
  {
    MapVector[] vectors = _values[i][index];
    scoredVectors.put(hammingDistance(_keys[i][index], hashes[i]), Arrays.asList(vectors));
    return vectors.length;
  }

  private LSHNode[] trimEmpty(List<LSHNode> nodes)
  {
    Iterator<LSHNode> it = nodes.iterator();
    while(it.hasNext())
    {
      LSHNode node = it.next();
      if(node._values.isEmpty()) it.remove();
    }
    return nodes.toArray(new LSHNode[nodes.size()]);
  }
  
  protected static final class LSHNode implements Comparable<LSHNode>
  {
    final long _key;
    final Set<MapVector> _values = new HashSet<MapVector>();
    
    LSHNode _left;
    LSHNode _right;

    LSHNode(long key, MapVector... vectors)
    {
      _key = key;
      for(MapVector v : vectors) _values.add(v);
    }
    

    public int compareTo(LSHNode o)
    {
      if(o._key == _key)
      {
        _values.addAll(o._values);
        o._values.clear();
      }
      return (int) (_key - o._key);
    }
  }
  
  public static final long hammingDistance(long x, long y)
  {
    long dist = 0;
    long val = x ^ y;
    while(val != 0)
    {
      ++dist;
      val &= (val - 1);
    }
    return dist;
  }
}
