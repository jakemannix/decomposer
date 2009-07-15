package org.decomposer.contrib.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.decomposer.math.vector.IntDoublePair;
import org.decomposer.math.vector.array.DenseMapVector;

public class DenseVectorWritableComparable implements WritableComparable<DenseVectorWritableComparable>
{
  protected long rowNum;
  protected DenseMapVector vector = new DenseMapVector();
  
  public long getRowNum() { return rowNum; }
  public void setRowNum(long rowNum) { this.rowNum = rowNum; }

  public DenseMapVector getVector() { return vector; }
  public void setVector(DenseMapVector vector) { this.vector = vector; }
  
  @Override
  public void readFields(DataInput in) throws IOException
  {
    rowNum = in.readLong();
    int size = in.readInt();
    double[] values = new double[size];
    for(int i=0; i<size; i++)
      values[i] = in.readDouble();
    vector = new DenseMapVector(values);
  }

  @Override
  public void write(DataOutput out) throws IOException
  {
    out.writeLong(rowNum);
    int size = vector.numNonZeroEntries();
    out.writeInt(size);
    for(IntDoublePair pair : vector)
      out.writeDouble(pair.getDouble());
  }

  @Override
  public int compareTo(DenseVectorWritableComparable o)
  {
    if(rowNum == o.rowNum) return 0;
    return rowNum > o.rowNum ? 1 : -1;
  }

}
