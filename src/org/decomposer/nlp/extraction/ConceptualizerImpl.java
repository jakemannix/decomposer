package org.decomposer.nlp.extraction;

import java.util.HashMap;
import java.util.Map;
import org.decomposer.math.EigenSpace;
import org.decomposer.math.vector.IntDoublePair;
import org.decomposer.math.vector.MapVector;
import org.decomposer.nlp.extraction.FeatureDictionary.Feature;

/**
 *
 * @author jmannix
 */
public class ConceptualizerImpl implements Conceptualizer
{
  protected final FeatureDictionary _dictionary;
  protected final FeatureExtractor _featureExtractor;
  protected final EigenSpace _eigenSpace;
  
  public ConceptualizerImpl(FeatureDictionary dictionary,
                            FeatureExtractor featureExtractor, 
                            EigenSpace eigenSpace)
  {
    _dictionary = dictionary;
    _featureExtractor = featureExtractor;
    _eigenSpace = eigenSpace;
  }
  
  public Map<String, Double> conceptualize(String inputText, int numTerms)
  {
    MapVector inputVector = _featureExtractor.extract(inputText);
    MapVector upliftedVector = _eigenSpace.expand(inputVector, numTerms);
    Map<String, Double> conceptTermVector = new HashMap<String, Double>(numTerms);
    Feature f;
    for(IntDoublePair pair : upliftedVector)
    {
      if((f = _dictionary.getFeature(pair.getInt())) != null) conceptTermVector.put(f.name, pair.getDouble());
    }
    return conceptTermVector;
  }
}
