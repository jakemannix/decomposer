package org.decomposer.contrib.hadoop.phrases;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
   * if you get A B C => [{B C:1234}, {A:1234567}, {A B C:12}, {C:12345}, {A B:2345}, {B:234567}]
   * you need F(A B C), F(A B), corpusSize : k1 = F(ABC), k2 = F(AB) - F(ABC), n1 = F(AB), n2 = corpusSize - F(AB)
   * 
   * if you need A B C D => ...
   * you need F(ABCD), F(ABC), corpusSize : k1 = F(ABCD), k2 = F(ABC) - F(ABCD), n1 = F(ABC), n2 = corpusSize - F(ABC)
   * @author jmannix
   *
   */
  public final class MLReducer extends Reducer<Text, Text, Text, FloatWritable>
  {
    FloatWritable value = new FloatWritable(1.0f);
    @Override
    public void reduce(Text ngram, Iterable<Text> jsons, Context context) throws IOException, InterruptedException
    {
      long totalNGrams = Long.parseLong(context.getConfiguration().get("ngrams.count"));
      String ngramString = ngram.toString();
      int lastSpace = ngramString.lastIndexOf(' ');
      if(lastSpace == -1)
      {
        // unigram case: -log(P(A)) = -log(F(A)/totalNGrams) = log(totalNGrams/F(A)) = idf(A) if you count each token as a "document"
        Text json = jsons.iterator().next();
        int count = count(json.toString());
        value.set((float) Math.log(((double)totalNGrams)/count));
      }
      else
      {
        String nMinus1Gram = ngramString.substring(0, lastSpace).trim();
        int nGramCount = 1;
        int nMinus1GramCount = 1;
        for(Text json : jsons)
        {
          String jsonS = json.toString();
          String subNGram = key(jsonS);
          if(nMinus1Gram.equals(subNGram))
          {
            nMinus1GramCount = count(jsonS);
          }
          else if(ngramString.equals(subNGram))
          {
            nGramCount = count(jsonS);
          }
          else
          {
            // not needed for this calculation - but maybe?
          }
        }
/*        
System.out.println("logLamda(" + nGramCount + ", " 
                               + nMinus1GramCount + " - " + nGramCount + ", " 
                               + nMinus1GramCount + ", " 
                               + totalNGrams + " - " + nMinus1GramCount + ")"); */
        Double v = minusLogLambda(nGramCount /*+ 1*/, 
                                  nMinus1GramCount - nGramCount /*+ 1*/, 
                                  nMinus1GramCount /*+ 1*/, 
                                  totalNGrams - nMinus1GramCount /*+ 1*/);
        value.set(v.floatValue());
        context.write(ngram, value);
      }
    }

    /**
     *  
        score (ABC)  = 2 L(p1,k1,n1)L(p2,k2,n2)/ ((p,k1,n1)(p,k2,n2))
                       k1 = F(ABC)
                       n1 = F (AB)
                       p1 = k1/n1 = F(ABC)/F(AB)
                       k2 = F(ABC) - F(AB)
                       n2 = BIGRAMS - F(AB)
                       p2 = k2/n2
                       p = k1+k2/n1+n2
                       L(p,k,n) = p^k*(1-p)^n-k
                       so Log(L(p,k,n)) = k*Log(p) + (n-k)Log(1-p)
         
         p, p1, p2, k1, k2, n1, n2
         (p1 = k1/n1, p2 = k2/n2, p = (k1+k2)/(n1+n2) )
         
         to score ABC, use:
         
         k1 = F(ABC), 
         k2 = F(AB) - F(ABC) (ie F(AB | ~ABC) )
         n1 = F(AB)
         n2 = num_bigrams - F(AB) (ie number of times you see a bigram which is *not* AB)
         
         similarly, for scoring AB, use:
         
         k1 = F(AB)
         k2 = F(A) - F(AB) (ie F(A | ~AB) )
         n1 = F(A)
         n2 = num_tokens - F(A) (ie number of token occurrences which are *not* A)
         
         
     * @author jmannix
     *
     */
    static double minusLogLambda(long k1, long k2, long n1, long n2)
    {
      if(k1 == k2 || k1 == 0 || k2 == 0)
      {
        // weird case? exact answer says you should return this:
        // return Math.log(n1 + n2);
        return Math.E;
      }
      double p1 = ((double)k1)/n1;
      double p2 = ((double)k2)/n2;
      double p = ((double)(k1+k2))/(n1+n2);
      // LogL(p1, k1, n1) - LogL(p, k1, n1) = k1*log(p1/p) + (n1 - k1)*log((1-p1)/(1-p)) = k1*log(k1/n1 / k1+k2/n1+n2) + (n1-k1)*log(1-k1/n1 / 1- k1+k2/n1+n2)
      return LogL(p1, k1, n1) + LogL(p2, k2, n2) - LogL(p, k1, n1) - LogL(p, k2, n2);
    }
    
    private static double LogL(double p, double k, double n)
    {
      return k * Math.log(p) + (n-k) * Math.log(1-p);
    }
    
    private static double logLikelihood(double freqA, double freqB, double freqAB, double numSamples)
    {
      return 2 * (crossLog(freqAB, freqA, freqB, numSamples) 
                + crossLog(freqA - freqAB, numSamples - freqA, freqB, numSamples) 
                + crossLog(freqB - freqAB, numSamples - freqB, freqA, numSamples)
                + crossLog(numSamples - freqA - freqB + freqAB, numSamples - freqA, numSamples - freqB, numSamples));
    }
    
    private static double crossLog(double w, double x, double y, double z)
    {
      return w * (Math.log(w) + Math.log(x) - Math.log(y) - Math.log(z));
    }
    
    
    private static String key(String json)
    {
      return json.substring(1,json.lastIndexOf(':'));
    }
    private static int count(String json)
    {
      return Integer.parseInt(json.substring(json.lastIndexOf(':')+1, json.length() - 1));
    }
  }