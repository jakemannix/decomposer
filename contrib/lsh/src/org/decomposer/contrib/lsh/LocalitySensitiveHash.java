package org.decomposer.contrib.lsh;

import org.decomposer.math.vector.MapVector;

/**
 *
 * @author jmannix
 */
public interface LocalitySensitiveHash 
{
  long[] hash(MapVector input);
}
