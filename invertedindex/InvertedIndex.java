package hadoop.invertedindex;


import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

  public static class InverterMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String file = ((FileSplit)context.getInputSplit()).getPath().getName();
      Set<String> tokenSet = new HashSet<>();
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String nextToken = itr.nextToken();
        if (!tokenSet.contains(nextToken)) {
            tokenSet.add(nextToken);
            context.write(new Text(nextToken), new Text(file));
        }
      }
    }
  }

  public static class InverterReducer
       extends Reducer<Text, Text,Text, Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      StringBuilder result = new StringBuilder();
      for (Text val : values) {
              System.out.println("KEY: " + key.toString() + "TEXT: " + val.toString());
              result.append(val);
              result.append(","); 
      }
      context.write(key, new Text(result.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(InvertedIndex.class);
    job.setMapperClass(InverterMapper.class);
//    job.setCombinerClass(InverterReducer.class);
    job.setReducerClass(InverterReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }


}

