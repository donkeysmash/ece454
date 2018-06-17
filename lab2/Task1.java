import java.io.IOException;
import java.util.StringTokenizer;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task1 {
  public static class Task1Mapper extends Mapper<Object, Text, Text, Text> {
    private Text title = new Text();
    private Text ratings = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",");
      LinkedList<String> idWithMaxRatings = new LinkedList<>();
      title.set(tokens[0]);
      int currentMax = 1;
      for (int i = 1; i < tokens.length; i++) {
        int current = 0;
        try {
          current = Integer.parseInt(tokens[i]);
        } catch (Exception e) { }
        if (current > currentMax) {
          idWithMaxRatings.clear();
          idWithMaxRatings.add(String.valueOf(i));
          currentMax = current;
        } else if (current == currentMax) {
          idWithMaxRatings.add(String.valueOf(i));
        }
      }
      ratings.set(String.join(",", idWithMaxRatings));
      context.write(title, ratings);
    }
  }



  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: task1 <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "task 1");
    job.setJar("Task1.jar");
    job.setMapperClass(Task1Mapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(0);
    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
