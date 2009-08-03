package org.decomposer.contrib.hadoop.job;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.decomposer.contrib.hadoop.math.MapVectorWritableComparable;
import org.decomposer.math.vector.DiskBufferedDoubleMatrix;
import org.decomposer.math.vector.DoubleMatrix;
import org.decomposer.math.vector.MapVector;

public class VectorSequenceFileFromSerializedBlobJob
{

  /**
   * usage: <progname> corpusDir eigenVectorDir outputDir
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws Exception
  {
    if(args.length != 3) usage();
    String corpusDir = args[0];
    String eigenVectorDir = args[1];
    String outputDir = args[2];
    
    Configuration conf = new Configuration();
    writeMatrixToSequenceFile(new DiskBufferedDoubleMatrix(new File(corpusDir), 1000), new Path(outputDir + "/corpus"), conf);
    writeMatrixToSequenceFile(new DiskBufferedDoubleMatrix(new File(eigenVectorDir), 1000), new Path(outputDir + "/eigenVectors"), conf);
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

  private static void usage()
  {
    System.out.println("<java with -jar invocation> corpusDir eigenVectorDir outputDir");
    System.exit(-1);
  }
}
