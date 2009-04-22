package org.decomposer.nlp.extraction;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

import org.decomposer.nlp.extraction.FeatureDictionary.Feature;
import org.decomposer.util.FileUtils;

import junit.framework.TestCase;

/**
 * @author jmannix
 *
 */
public class FeatureDictionaryTest extends TestCase
{    
  private static final Logger log = Logger.getLogger(FeatureDictionaryTest.class.getName());
  private static final char[] alpha = new char[] { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r' };
  protected FeatureDictionary _dictionary;
  protected long _seed = 1234982346987L;
  
  protected Iterable<Map<String, Double>> buildCorpus(int numDocs, int numTermsPerDoc) throws Exception
  {
    List<Map<String, Double>> corpus = new ArrayList<Map<String, Double>>(numDocs);
    for(int i=0; i<numDocs; i++)
    {
      Map<String, Double> doc = new HashMap<String, Double>(numTermsPerDoc);
      for(int j=0; j<numTermsPerDoc; j++)
      {
        String term = getTerm();
        Double value = doc.get(term);
        doc.put(term, value != null ? value + 1 : 1);
      }
      corpus.add(doc);
    }
    return corpus;
  }
  
  private String getTerm()
  {
    Random r = new Random(_seed * _seed++);
    char[] chars = new char[2];
    for(int i=0; i<chars.length; i++)
    {
      chars[i] = alpha[r.nextInt(alpha.length)];
    }
    return new String(chars);
  }

  /**
   * @param name
   */
  public FeatureDictionaryTest(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    super.setUp();
  }

  protected void tearDown() throws Exception
  {
    super.tearDown();
  }
  
  public void testDictionaryConstruction() throws Exception
  {
    FeatureDictionary dictionary = new FeatureDictionary(buildCorpus(10, 15));
    for(Map<String, Double> document : buildCorpus(1, 20))
    {
      for(String s : document.keySet())
      {
        Feature f = dictionary.getFeature(s);
        if(f != null)
        {
   //       log.info(s + " is in! - id = " + f.id + ", count = " + f.count);
        }
        else
        {
   //       log.info(s + " not in!");
        }
      }
    }
    File tmpFile = FileUtils.createTmpDir(getName(), true);
    FileUtils.serialize(dictionary, tmpFile, "dictionary.ser");
    
    FeatureDictionary dictionary2 = FileUtils.deserialize(FeatureDictionary.class, 
                                                          new File(tmpFile.getPath() + File.separator + "dictionary.ser"));
    assertNotNull(dictionary2);
  }
}
