package org.decomposer.contrib.hadoop.math;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.decomposer.math.vector.IntDoublePair;

public class IntDoublePairWritableComparable implements WritableComparable<IntDoublePairWritableComparable>, IntDoublePair
{
  private int i;
  private double d;
  public IntDoublePairWritableComparable(int i, double d)
  {
    this.i = i;
    this.d = d;
  }
  
  public IntDoublePairWritableComparable()
  {
    i = 0;
    d = 0;
  }

  @Override
  public double getDouble() { return d; }
  @Override
  public int getInt() { return i; }
  
  public void setInt(int i) { this.i = i; }

  @Override
  public void setDouble(double newValue) { d = newValue; }

  @Override
  public void readFields(DataInput in) throws IOException
  {
    i = in.readInt();
    d = in.readDouble();
  }

  @Override
  public void write(DataOutput out) throws IOException
  {
    out.writeInt(i);
    out.writeDouble(d);
  }

  @Override
  public int compareTo(IntDoublePairWritableComparable o)
  {
    if(i == o.i) return 0;
    return i > o.i ? 1 : -1;
  }

}
