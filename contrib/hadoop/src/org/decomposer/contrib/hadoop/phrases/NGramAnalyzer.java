/**
 * 
 */
package org.decomposer.contrib.hadoop.phrases;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
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
    TokenStream result = new NGramFilter(new LowerCaseFilter(new StandardFilter(t)));
    String[] stops = new String[StandardAnalyzer.STOP_WORDS.length + stopWords.length];
    System.arraycopy(StandardAnalyzer.STOP_WORDS, 0, stops, 0, StandardAnalyzer.STOP_WORDS.length);
    System.arraycopy(stopWords, 0, stops, StandardAnalyzer.STOP_WORDS.length, stopWords.length);
    result = new StopFilter(result, stops);
    result = new ShingleFilter(result, max);
    return result;
  }
  
  private static final class NGramFilter extends TokenFilter
  {
    protected NGramFilter(TokenStream input)
    {
      super(input);
    }
    
    @Override
    public Token next(Token token) throws IOException
    {
      Token next = input.next(token);
      while(next != null)
      {
        boolean hasAlpha = false;
        char[] termBuffer = next.termBuffer();
        int len = next.termLength();
        for(int i = 0; i < len; i++)
        {
          if(Character.isLetter(termBuffer[i]))
          {
            hasAlpha = true;
            break;
          }
        }
        if(!hasAlpha || next.termLength() < 2)
        {
          next = input.next(next);
        }
        else
        {
          break;
        }
      }
      return next;
    }
    
  }
  
  public static final String[] stopWords = 
  {
    "have", "has", "been", "than", "he", "she", "it", "him", "her", "their", "his", "hers", "theirs", "our", "who", 
    "mo", "tu", "we", "th", "fr", "sa", "su", "what", "when", "where", "why", "sos", "wikipedia", "title", "subject", "children",
    "info@soschildren.org", "return", "charity", "villages", "donate", "link", "from", "site"
  };
  
}