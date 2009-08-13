package org.decomposer.contrib.hadoop.job;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.util.ToolRunner;
import org.decomposer.contrib.hadoop.BaseTool;
import org.decomposer.contrib.hadoop.math.MapVectorWritableComparable;
import org.decomposer.math.vector.DiskBufferedDoubleMatrix;
import org.decomposer.math.vector.DoubleMatrix;
import org.decomposer.math.vector.MapVector;

public class VectorSequenceFileFromSerializedBlobJob extends BaseTool
{

  /**
   * usage: <progname> corpusDir eigenVectorDir outputDir
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws Exception
  {
    int ret = ToolRunner.run(new VectorSequenceFileFromSerializedBlobJob(), args);
    System.exit(ret);
  }
  
  public static void writeMatrixToSequenceFile(DoubleMatrix matrix, Path outputDir, Configuration conf) throws Exception
  {
    FileSystem fs = FileSystem.get(conf);
    
    SequenceFile.Writer writer = new Writer(fs, 
                                            conf, 
                                            outputDir, 
                                            LongWritable.class, 
                                            MapVectorWritableComparable.class);
    MapVectorWritableComparable vectorWritable = new MapVectorWritableComparable(outputDir);
    LongWritable key = new LongWritable();
    for(Entry<Integer, MapVector> vector : matrix)
    {
      vectorWritable.setRow(vector.getKey());
      vectorWritable.setVector(vector.getValue());
      key.set(vector.getKey());
      writer.append(key, vectorWritable);
    }
    
  }

  @Override
  public int run(String[] args) throws Exception
  {
    Configuration conf = getConf();
    conf.setBoolean("is.local", true);
    conf.set("job.name", System.currentTimeMillis() + "/");
    Properties configProps = loadJobProperties();
    
    String inputDir = configProps.getProperty("input.matrix.blob.dir");
    String outputPath = configProps.getProperty("output.path");
    
    writeMatrixToSequenceFile(new DiskBufferedDoubleMatrix(new File(inputDir), 1000), new Path(outputPath), conf);
    return 1;
  }
}
