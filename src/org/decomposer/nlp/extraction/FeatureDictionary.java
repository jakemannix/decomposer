package org.decomposer.nlp.extraction;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Dead simple bi-directional map (String to Integer) for translating tokens in text into columns ids in a term-document matrix.  
 * Also accessible, along with the id of a token, is the count of the number of rows with a nonzero value on this column.
 * @author jmannix
 * @see org.decomposer.nlp.extraction.FeatureExtractor
 */
public class FeatureDictionary implements Serializable
{
  private static final long serialVersionUID = 1L;
  
  protected Map<String, Feature> _featuresByName = new HashMap<String, Feature>();
  protected Map<Integer, Feature> _featuresById = new HashMap<Integer, Feature>();
  protected Integer _maxId = 0;
  protected int _numDocs = 0;
  
  public FeatureDictionary()
  {
    
  }
  
  public FeatureDictionary(Iterable<? extends Map<String, Double>> documents)
  {
    for(Map<String, Double> document : documents) updateFeature(document);
  }
  
  public int getNumFeatures()
  {
    return _maxId;
  }
  
  public Feature getFeature(int id)
  {
    return _featuresById.get(id);
  }
  
  public Feature getFeature(String name)
  {
    return _featuresByName.get(name);
  }
  
  public int getNumDocs()
  {
    return _numDocs;
  }
  
  public void updateFeature(Map<String, Double> document)
  {
    for(String feature : document.keySet())
    {
      updateFeature(feature, document.get(feature));
    }
    _numDocs++;
  }
  
  public void updateFeature(String feature, Double weight)
  {
    Feature f = getFeature(feature);
    if(f == null)
    {
      f = new Feature();
      f.name = feature;
      f.id = _maxId;
      f.count = weight.intValue();
      _maxId++;
      _featuresByName.put(feature, f);
      _featuresById.put(f.id, f);
      if(_numDocs < f.count) _numDocs = f.count + 1;
    } 
    f.count++;
  }
  
  public static class Feature implements Serializable
  {
    private static final long serialVersionUID = 1L;
    public String name;
    public Integer id;
    public Integer count;
  }
}
