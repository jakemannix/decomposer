package org.decomposer.nlp.extraction;

import org.decomposer.math.vector.MapVector;
import org.decomposer.math.vector.VectorFactory;
import org.decomposer.math.vector.array.ImmutableSparseMapVector;
import org.decomposer.math.vector.hashmap.HashMapVectorFactory;
import org.decomposer.nlp.extraction.FeatureDictionary.Feature;

/**
 *
 * @author jmannix
 */
public class DelimitedDictionaryFeatureExtractor implements FeatureExtractor
{
  protected final FeatureDictionary _featureDictionary;
  protected final String _delimiter;
  protected final Idf _idf;
  protected final VectorFactory _vectorFactory = new HashMapVectorFactory();
  
  public DelimitedDictionaryFeatureExtractor(FeatureDictionary featureDictionary)
  {
    this(featureDictionary, " ");
  }
  
  public DelimitedDictionaryFeatureExtractor(final FeatureDictionary featureDictionary,
                                             String delimiter)
  {
    this(featureDictionary, delimiter, new Idf() 
    {
      public double idf(int count) 
      {
        return Math.log((featureDictionary.getNumFeatures() + 1) / (count + 1));
      }
    });
  }
  
  public DelimitedDictionaryFeatureExtractor(FeatureDictionary featureDictionary,
                                             String delimiter,
                                             Idf idf)
  {
    _featureDictionary = featureDictionary;
    _delimiter = delimiter;
    _idf = idf;
  }
    
  public MapVector extract(String inputText) 
  {
    MapVector vector = _vectorFactory.zeroVector();
    String[] tokens = inputText.split(_delimiter);
    for(String token : tokens)
    {
      Feature feature = _featureDictionary.getFeature(token);
      if(feature != null) vector.add(feature.id, _idf.idf(feature.count));
    }
    return new ImmutableSparseMapVector(vector);
  }

}
