import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task4 {
	private static final String OUTPUT_PATH = "intermediate_output_test";
  	public static class Task4Mapper extends Mapper<Object,Text,Text,MapWritable> {
   		private Text userId = new Text();
   		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	   		String[] tokens = value.toString().split(",");
	   		String movieName = tokens[0];
	   		
	   		for (int i = 1; i < tokens.length; i++) {
	   			if(tokens[i].length() != 0 ) {
	   				MapWritable mymap = new MapWritable();
	   				userId.set(String.valueOf(i)); 
	   				IntWritable val = new IntWritable(Integer.parseInt(tokens[i]));
	   				mymap.put(new Text(movieName), val);
					context.write(userId, mymap); 
		   		} 
			}
	   	}
   	}

   	public static class Task4Combiner extends Reducer<Text,MapWritable,Text,MapWritable> {
   		private Text movies = new Text();
   		private final static IntWritable one = new IntWritable(1);
   		private final static IntWritable zero = new IntWritable(0);
    	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

			MapWritable maps1 = new MapWritable();
			MapWritable maps2 = new MapWritable();
			for (MapWritable value : values) {
				for (Map.Entry<Writable,Writable> entry: value.entrySet()) {
					maps1.put(entry.getKey(), entry.getValue());
					maps2.put(entry.getKey(), entry.getValue());
				}				
			}

			int i = 0;
			for (Map.Entry<Writable,Writable> entry1: maps1.entrySet()) {

				Text word1 = (Text)entry1.getKey();
				MapWritable mymap = new MapWritable();
				IntWritable valFirst = (IntWritable)entry1.getValue();
				int j = 0;
				for (Map.Entry<Writable,Writable> entry2: maps2.entrySet()) {
					if (j > i) {
						IntWritable valSecond = (IntWritable)entry2.getValue();
						Text word2 = (Text)entry2.getKey();
						if (valFirst.get() == valSecond.get()) {
							mymap.put(word2, one);
						} else {
							mymap.put(word2, zero);
						}
					}					
					j++;
				}
				context.write(word1, mymap);
				i++;
			}

    	}
   	}

   	public static class Task4Reducer extends Reducer<Text,MapWritable,Text,IntWritable> {
   					private Text pair = new Text();

   		private IntWritable result = new IntWritable();
    	public void reduce(Text word1, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
			MapWritable map = new MapWritable();
			for (MapWritable value: values) {
			    for (Map.Entry<Writable,Writable> entry: value.entrySet()) {
					Text word2 = (Text)entry.getKey();
					IntWritable newCount = (IntWritable)entry.getValue();
					IntWritable count = (IntWritable)map.get(word2);
					if (count == null)
					    count = newCount;
					else
					    count = new IntWritable(count.get() + newCount.get());
					map.put(word2, count);
			    }
			}
			for (Map.Entry<Writable,Writable> entry: map.entrySet()) {
			    Text word2 = (Text)entry.getKey();
			    pair.set(word1.toString() + ":" + word2.toString());
			    context.write(pair, (IntWritable)entry.getValue());
			}
    	}
   	}

   	public static class Task4Mapper2 extends Mapper<Object,Text,Text,IntWritable> {
   		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	   		String[] tokens = value.toString().split(",");
	   		
	   		String[] movies = tokens[0].split(":");
	   		String movieName = movies[0] + "," + movies[1];
	   		
   			if(tokens.length > 1) {
				context.write(new Text(movieName), new IntWritable(Integer.parseInt(tokens[1]))); 
	   		} 
			
	   	}
   	}

   	public static class Task4Reducer2 extends Reducer<Text,IntWritable,Text,IntWritable> {
   					private Text pair = new Text();

   		private IntWritable result = new IntWritable();
    	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
		    for (IntWritable val : values) {
		        sum += val.get();
		    }
		    result.set(sum);
		    context.write(key, result);
    	}
   	}

	public static void main(String[] args) throws Exception {
	  	Configuration conf = new Configuration();
	    conf.set("mapred.textoutputformat.separator", ",");
	    
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: wordcount <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "word count");
	    job.setJar("Task4.jar");

	    job.setMapperClass(Task4Mapper.class);
	    job.setCombinerClass(Task4Combiner.class);
	    job.setReducerClass(Task4Reducer.class);

	    job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    TextOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

	    job.waitForCompletion(true);

	    Job job2 = new Job(conf, "Job 2");
	    job2.setJar("Task4.jar");

		job2.setMapperClass(Task4Mapper2.class);
		job2.setReducerClass(Task4Reducer2.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		TextInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
		TextOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
	    
	    System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}

}

