/**
 * 
 */
package org.decomposer.contrib.hadoop.phrases;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;


public class TextAndInt implements WritableComparable<TextAndInt>
{
  int n = -1;
  Text text = new Text();
  public TextAndInt() {}
  public TextAndInt(int n, String s) 
  { 
    this.n = n;
    text.set(s);
  }
  public int getN() { return n; }
  public String toString() { return text.toString() + "\t" + n; }
  public String toJson() { return "{" + text.toString() + " : " + n + "}"; }
  
  public boolean equals(Object o)
  {
    if(o instanceof TextAndInt)
    {
      TextAndInt other = (TextAndInt)o;
      return n == other.n && text.equals(other.text);
    }
    return false;
  }
  
  public int hashCode()
  {
    return text.hashCode() ^ (127 * n);
  }
  
  public void readFields(DataInput in) throws IOException
  {
    text.readFields(in);
    n = WritableUtils.readVInt(in);
  }
  public void write(DataOutput out) throws IOException
  {
    text.write(out);
    WritableUtils.writeVInt(out, n);
  }
  public int compareTo(TextAndInt o)
  {
    int textCompare = text.compareTo(o.text);
    if(textCompare == 0)
    {
      return -(n - o.n);
    }
    return textCompare;
  }

  public static class TextAndIntComparator extends WritableComparator 
  {
    public TextAndIntComparator() 
    {
      super(TextAndInt.class);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) 
    {
      return compareBytes(b1, s1, l1, b2, s2, l2);
    }
  }
  
  static
  {
    WritableComparator.define(TextAndInt.class, new TextAndIntComparator());
  }
}