package org.decomposer.contrib.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.decomposer.math.vector.IntDoublePair;
import org.decomposer.math.vector.array.ImmutableSparseMapVector;

public class SparseVectorWritableComparable implements WritableComparable<SparseVectorWritableComparable>
{
  protected long rowNumber;
  protected ImmutableSparseMapVector vector;
  
  public void setRow(long row) { rowNumber = row; }
  public void setVector(ImmutableSparseMapVector sparseVector) { vector = sparseVector; }
  public long getRow() { return rowNumber; }
  public ImmutableSparseMapVector getVector() { return vector; }
  
  @Override
  public void readFields(DataInput in) throws IOException
  {
    rowNumber = in.readLong();
    int size = in.readInt();
    int[] indexes = new int[size];
    double[] values = new double[size];
    for(int i=0; i<size; i++)
    {
      indexes[i] = in.readInt();
      values[i] = in.readDouble();
    }
    vector = new ImmutableSparseMapVector(indexes, values);
  }

  @Override
  public void write(DataOutput out) throws IOException
  {
    out.writeLong(rowNumber);
    int size = vector.numNonZeroEntries();
    out.write(size);
    for(IntDoublePair pair : vector)
    {
      out.write(pair.getInt());
      out.writeDouble(pair.getDouble());
    }
  }

  @Override
  public int compareTo(SparseVectorWritableComparable o)
  {
    if(o.rowNumber == rowNumber) return 0;
    return o.rowNumber > rowNumber ? 1 : -1;
  }

}
