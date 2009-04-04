package org.decomposer.nlp.extraction;

import org.decomposer.math.vector.MapVector;

/**
 * Translation interface, for turning document text into a {@link org.decomposer.math.vector.MapVector MapVector}
 * @author jmannix
 */
public interface FeatureExtractor 
{
  /**
   * 
   * @param inputText
   * @return {@link org.decomposer.math.vector.MapVector MapVector} representing the inputText
   */
  MapVector extract(String inputText);
}
