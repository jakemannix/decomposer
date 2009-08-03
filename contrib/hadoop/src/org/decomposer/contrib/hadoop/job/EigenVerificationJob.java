package org.decomposer.contrib.hadoop.job;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.util.ToolRunner;
import org.decomposer.contrib.hadoop.BaseTool;
import org.decomposer.contrib.hadoop.math.DistributedMatrix;
import org.decomposer.math.vector.MapVector;

public class EigenVerificationJob extends BaseTool
{

  public static void main(String[] args) throws Exception
  {
    int ret = ToolRunner.run(new EigenVerificationJob(), args);
    System.exit(ret);
  }
  
  @Override
  public int run(String[] args) throws Exception
  {
    Configuration conf = getConf();
    conf.setBoolean("is.local", true);
    conf.set("job.name", System.currentTimeMillis() + "/");
    Properties configProps = loadJobProperties();
    
    DistributedMatrix corpus = new DistributedMatrix(new Path(configProps.getProperty("corpus.path")));
    corpus.setConf(getConf());
    DistributedMatrix eigenVectors = new DistributedMatrix(new Path(configProps.getProperty("eigenVector.path")));
    eigenVectors.setConf(getConf());
   
    List<String> outputStrings = new ArrayList<String>();
    for(Entry<Integer, MapVector> eigenVector : eigenVectors)
    {
      MapVector eigen = eigenVector.getValue();
      MapVector afterMultiply = corpus.timesSquared(eigen);
      double error = 1 - (afterMultiply.dot(eigen) / (afterMultiply.norm() * eigen.norm()));
      double eigenValue = afterMultiply.norm();
      String evalString = "Eigenvector(" + eigenVector.getKey() + ") has eigenValue = " + eigenValue +", and error = " + error;
      outputStrings.add(evalString);
    }
    
    SequenceFile.Writer writer = new Writer(FileSystem.get(getConf()), 
                                            getConf(), 
                                            new Path((new Path(configProps.getProperty("eigenVector.path")).getParent()), "errors"),
                                            NullWritable.class,
                                            Text.class);
    for(String string : outputStrings)
    {
      writer.append(NullWritable.get(), new Text(string));
    }
    writer.close();
    return 1;
  }
  
  
}
