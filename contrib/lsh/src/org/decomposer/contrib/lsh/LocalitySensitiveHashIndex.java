package org.decomposer.contrib.lsh;

import java.util.List;
import org.decomposer.math.vector.MapVector;

/**
 *
 * @author jmannix
 */
public interface LocalitySensitiveHashIndex 
{
  
  void add(MapVector docVector);
  
  List<MapVector> findNearest(MapVector queryVector);
  
  List<MapVector> findNearest(MapVector queryVector, int numResults);

}
