package org.decomposer.contrib.lucene;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.store.Directory;

import org.decomposer.nlp.extraction.BoundedFeatureDictionary;
import org.decomposer.nlp.extraction.FeatureDictionary;
import org.decomposer.nlp.extraction.Idf;

public class LuceneIndexDictionaryBuilder
{
  public static FeatureDictionary buildDictionary(Directory directory, 
                                                  Term term, 
                                                  Idf idf, 
                                                  float minIdf, 
                                                  int maxTerms)
  {
    FeatureDictionary dictionary = new BoundedFeatureDictionary(maxTerms);
    
    try
    {
      IndexReader reader = IndexReader.open(directory);
      int maxDoc = reader.maxDoc();
      int numDocs = reader.numDocs();
      for(int i=0; i<maxDoc; i++)
      {
        if(reader.isDeleted(i)) continue;
        TermFreqVector termVector = reader.getTermFreqVector(i, term.field());
        if(termVector == null)
        {
          //
          continue;
        }
        int[] freqs = termVector.getTermFrequencies();
        String[] terms = termVector.getTerms();
        Map<String, Double> filteredTermVector = new HashMap<String, Double>();
        for(int j=0; j<terms.length; j++)
        {
          if(idf.idf(numDocs, reader.docFreq(new Term(term.field(), terms[j]))) > minIdf)
          {
            filteredTermVector.put(terms[j], (double)freqs[j]);
          }
        }
        dictionary.updateFeature(filteredTermVector);
      }
    }
    catch (CorruptIndexException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    return dictionary;
  }
}
