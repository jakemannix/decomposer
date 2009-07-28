/**
 * 
 */
package org.decomposer.contrib.hadoop.phrases;

import java.io.IOException;
import java.io.Reader;

import org.apache.hadoop.io.Text;

public class TextReader extends Reader
{
  final Text text;
  int current = 0;
  public TextReader(Text text) { this.text = text; }
  
  @Override
  public void close() throws IOException
  {
    // NO-OP
  }
  
  public char nextChar()
  {
    return (char) text.charAt(current++);
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException
  {     
    if ((off < 0) || (off > cbuf.length) || (len < 0) || ((off + len) > cbuf.length) || ((off + len) < 0)) 
    {
      throw new IndexOutOfBoundsException();
    } 
    else if (len == 0) 
    {
      return 0;
    }
    else if (current >= text.getLength()) return -1;
    int n = Math.min(text.getLength() - current, len);
    int i = 0;
    while(i < n)
    {
      cbuf[off + i++] = nextChar();
    }
    return n;
  }
  
  
  @Override
  public int read(char[] cbuf) throws IOException
  {
    return super.read(cbuf);
  }
  
}