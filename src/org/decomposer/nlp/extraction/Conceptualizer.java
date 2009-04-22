/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

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
