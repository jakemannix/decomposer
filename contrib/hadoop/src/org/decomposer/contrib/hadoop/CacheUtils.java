package org.decomposer.contrib.hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CacheUtils
{
  public static FSDataInputStream getLocalCacheFile(Configuration config, String fileName) throws IOException 
  {
    Path[] cacheFiles = DistributedCache.getLocalCacheFiles(config);
    FileSystem fs = FileSystem.getLocal(config);
File f = new File(fileName);
return new FSDataInputStream(new FileInputStream(f.listFiles()[0]));
 //   for(Path path : cacheFiles) if(path.getName().equals(fileName)) return fs.open(path);
 //   throw new FileNotFoundException(fileName);
  }
}
