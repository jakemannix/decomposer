package org.decomposer.nlp.extraction;

/**
 *
 * @author jmannix
 */
public interface Idf 
{
  double idf(int numDocs, int count);
}
