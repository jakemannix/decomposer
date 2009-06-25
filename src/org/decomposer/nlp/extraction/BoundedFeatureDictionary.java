package org.decomposer.nlp.extraction;


/**
 *
 * @author jmannix
 */
public class BoundedFeatureDictionary extends FeatureDictionary
{
  private static final long serialVersionUID = 1L;
  protected final int _maxFeatures;
  
  public BoundedFeatureDictionary(int maxFeatures)
  {
    _maxFeatures = maxFeatures;
  }
  
  @Override
  public void updateFeature(String feature, Double weight)
  {
    if(getNumFeatures() <= _maxFeatures)
    {
      super.updateFeature(feature, weight);
    }
    else
    {    
      Feature f = getFeature(feature);
      if(f != null) f.count++;
    }
  }
}
