package edu.berkeley.cs.succinct.buffers.annot.examples;

import com.elsevier.cat.StringArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Arrays;

public class Construct {
  public static void main(String[] args)
    throws IOException, IllegalAccessException, InstantiationException {
    if (args.length != 2) {
      System.err.println("Parameters: [input-file] [output-path]");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    Path path = new Path(args[0]);
    SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
    Text key = (Text) reader.getKeyClass().newInstance();
    StringArrayWritable value = (StringArrayWritable) reader.getValueClass().newInstance();

    while (reader.next(key, value)) {
      System.out.println("Key: " + key.toString() + " Value: " + Arrays.toString(value.toStrings()));
    }
  }
}
