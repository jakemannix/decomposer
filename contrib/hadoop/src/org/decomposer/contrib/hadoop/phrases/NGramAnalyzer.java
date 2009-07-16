/**
 * 
 */
package org.decomposer.contrib.hadoop.phrases;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class NGramAnalyzer extends Analyzer
{
  private final int max;
  public NGramAnalyzer(int maxN)
  {
    max = maxN;
  }
  @Override
  public TokenStream tokenStream(String arg0, Reader reader)
  {
    StandardTokenizer t = new StandardTokenizer(reader);
    t.setMaxTokenLength(100);
    TokenStream result = new LowerCaseFilter(new StandardFilter(t));
    result = new StopFilter(result, StandardAnalyzer.STOP_WORDS);
    result = new ShingleFilter(result, max);
    return result;
  }
}