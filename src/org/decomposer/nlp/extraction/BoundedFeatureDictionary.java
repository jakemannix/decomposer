/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.decomposer.nlp.extraction;


/**
 *
 * @author jmannix
 */
public class BoundedFeatureDictionary extends FeatureDictionary
{
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
