package org.decomposer.nlp.extraction;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author jmannix
 */
public class FeatureDictionary implements Serializable
{
  private static final long serialVersionUID = 1L;
  
  protected Map<String, Feature> _featuresByName = new HashMap<String, Feature>();
  protected Map<Integer, Feature> _featuresById = new HashMap<Integer, Feature>();
  protected Integer _maxId = 0;
  
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
  
  public void updateFeature(String feature)
  {
    Feature f = getFeature(feature);
    if(f == null)
    {
      f = new Feature();
      f.name = feature;
      f.id = _maxId;
      f.count = 0;
      _maxId++;
      _featuresByName.put(feature, f);
      _featuresById.put(f.id, f);
    }
    f.count++;
  }
  
  public static class Feature
  {
    String name;
    Integer id;
    Integer count;
  }
}
