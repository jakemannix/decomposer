package org.decomposer.contrib.lucene;

import java.io.IOException;
import java.io.StringReader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.decomposer.math.vector.MapVector;
import org.decomposer.math.vector.VectorFactory;
import org.decomposer.math.vector.array.ImmutableSparseMapVector;
import org.decomposer.math.vector.hashmap.HashMapVectorFactory;
import org.decomposer.nlp.extraction.FeatureDictionary;
import org.decomposer.nlp.extraction.FeatureDictionary.Feature;
import org.decomposer.nlp.extraction.FeatureExtractor;
import org.decomposer.nlp.extraction.Idf;

/**
 *
 * @author jmannix
 */
public class LuceneFeatureExtractor implements FeatureExtractor
{
  protected final FeatureDictionary _dictionary;
  protected final Analyzer _analyzer;
  protected final Idf _idf;
  protected final VectorFactory _vectorFactory = new HashMapVectorFactory();
  
  public LuceneFeatureExtractor(FeatureDictionary dictionary,
                                Analyzer analyzer,
                                Idf idf)
  {
    _dictionary = dictionary;
    _analyzer = analyzer;
    _idf = idf;
  }
  
  public MapVector extract(String inputText)
  {
    MapVector v = _vectorFactory.zeroVector();
    TokenStream stream = _analyzer.tokenStream("", new StringReader(inputText));
    Token t = new Token();
    try
    {
      while ((t = stream.next(t)) != null)
      {
        Feature feature = _dictionary.getFeature(t.term());
        if(feature != null) v.add(feature.id, _idf.idf(_dictionary.getNumDocs(), feature.count));
      }
    }
    catch (IOException ex)
    {
      // TODO
    }
    return new ImmutableSparseMapVector(v);
  }

}
