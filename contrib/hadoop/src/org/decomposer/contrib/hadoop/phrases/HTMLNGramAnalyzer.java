package org.decomposer.contrib.hadoop.phrases;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.Text;
import org.apache.html.dom.HTMLDocumentImpl;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.cyberneko.html.parsers.DOMFragmentParser;
import org.w3c.dom.DOMException;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.html.HTMLDocument;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class HTMLNGramAnalyzer extends NGramAnalyzer
{
  DOMFragmentParser parser = new DOMFragmentParser();
  HTMLDocument document = new HTMLDocumentImpl();
  
  public HTMLNGramAnalyzer(int maxN)
  {
    super(maxN);
  }
  
  public TokenStream tokenStream(Text input)
  {
    DocumentFragment fragment = document.createDocumentFragment();
    try
    {
      parser.parse(new InputSource(new HTMLTextReader(input)), fragment);
      String fragText = fragment.getTextContent();
      return super.tokenStream("", new StringReader(fragText));
    }
    catch(DOMException d)
    {
      System.out.println("DamnDOM!");
    }
    catch (SAXException e)
    {
      System.out.println("DamnSAX!");
    }
    catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return new TokenStream()
    {
      @Override
      public Token next(Token t) { return null; }
    };
  }
  
  public static class HTMLTextReader extends TextReader
  {
    private boolean hasInsertedSpace = false;
    private final static String[][] startsAndEnds = 
    {
      {"<![CDATA[", "]]>"},
      {"<!--",  "-->"},
      {"<head>", "</head>"},
      {"<div class=\"printfooter\">", "</div>"},
      {"<div id=\"sosebar\">", "</div>"},
      {"<h3 id=\"siteSub\">", "</h3>"}
    };
    
    private char previousChar;
    
    public HTMLTextReader(Text text)
    {
      super(text);
      for(String[] startAndEnd : startsAndEnds) blankOut(startAndEnd[0], startAndEnd[1]);
    }
    
    private void blankOut(String start, String end)
    {
      int startIndex = -1;
      while((startIndex = text.find(start, startIndex + 1)) > 0)
      {
        int endIndex = text.find(end, startIndex + 1);
        if(endIndex < 0) 
        {
          endIndex = text.getLength();
        }
        else
        {
          endIndex += end.length();
        }
        byte[] textBytes = text.getBytes();
        for(int i = startIndex; i<endIndex; i++) textBytes[i] = ' ';
      }
    }
    
    public char nextChar()
    {
      char currentChar = (char) text.charAt(current);
      if(previousChar == '>' && !hasInsertedSpace)
      {
        hasInsertedSpace = true;
        previousChar = currentChar;
        return ' ';
      }
      else
      {
        hasInsertedSpace = false;
        current++;
        previousChar = currentChar;
        return currentChar;
      }
    }
    
  }

}
