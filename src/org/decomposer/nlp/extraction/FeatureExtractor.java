package org.decomposer.nlp.extraction;

import org.decomposer.math.vector.MapVector;
/**
 *
 * @author jmannix
 */
public interface FeatureExtractor 
{
  MapVector extract(String inputText);
}
