package org.decomposer.nlp.extraction;

import java.util.Map;

/**
 *
 * @author jmannix
 */
public interface Conceptualizer 
{

  Map<String, Double> conceptualize(String inputText, int numTerms);
  
}
