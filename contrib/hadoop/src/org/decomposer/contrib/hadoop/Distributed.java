package org.decomposer.contrib.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.Path;

public interface Distributed extends Configurable
{
  Path getPath();
  void localize() throws IOException;
  void distribute() throws IOException;
}
