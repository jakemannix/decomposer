package org.decomposer.nlp.extraction;

import java.util.Map;

/**
 * This is an interface for "query expansion" - input text, get out a weighted word-bag of related text.
 * @author jmannix
 */
public interface FeatureExpander 
{

  /**
   * 
   * @param inputText
   * @return wordbag of related terms to the input text.
   */
  Map<String, Double> expand(String inputText);
  
}
