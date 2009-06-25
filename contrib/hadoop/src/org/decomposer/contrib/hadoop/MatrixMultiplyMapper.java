package org.decomposer.contrib.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixMultiplyMapper extends Mapper<Object, SparseVectorWritableComparable, NullWritable, DenseVectorWritableComparable>
{
  protected DenseVectorWritableComparable inputVector = new DenseVectorWritableComparable();
  protected DenseVectorWritableComparable output = new DenseVectorWritableComparable();
  
  /**
   * Loads up, from the DistributedCache, the previous vector (which you are multiplying by)
   * @param context
   * @throws IOException
   */
  protected void loadVectors(Configuration config) throws IOException
  {
    Path[] cacheFiles = DistributedCache.getLocalCacheFiles(config);
    FileSystem fs = FileSystem.getLocal(config);
    inputVector.readFields(fs.open(cacheFiles[0]));
  }
  
  @Override
  public void setup(Context context) throws IOException, InterruptedException
  {
    loadVectors(context.getConfiguration());
  }


  /**
   * input value is the row of the matrix to multiply by.  No output yet, just accumulate!
   */
  @Override
  public void map(Object key, SparseVectorWritableComparable value, Context context) throws IOException, InterruptedException
  {
    double dot = value.getVector().dot(inputVector.getVector());
    output.getVector().plus(value.getVector(), dot);
  }
  
  /**
   * when we're done with all of the rows accessible to this Mapper, we finally write out the accumulatedOutput
   */
  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException 
  {
    context.write(NullWritable.get(), output);
  }
}
