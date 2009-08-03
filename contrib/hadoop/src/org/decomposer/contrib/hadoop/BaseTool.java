package org.decomposer.contrib.hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public abstract class BaseTool extends Configured implements Tool
{
  
  protected Properties loadJobProperties() throws FileNotFoundException, IOException
  {
    Properties props = new Properties();
    File propsFile = new File(getConf().get("conf.dir", "contrib/hadoop/resources") + File.separatorChar + getClass().getSimpleName() + ".props");
    props.load(new FileInputStream(propsFile));
    return props;
  }
  
  public static String getCurrentTimeStamp()
  {
    String timestamp = new Date().toString().replace(' ', '_').replace(':', '_');
    return timestamp;
  }
}
