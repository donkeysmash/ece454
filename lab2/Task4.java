import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.net.URI;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Task4 {
	private static final String OUTPUT_PATH = "intermediate_output";
	private static final String CACHE_PATH = "cache";

	public static class Task4Mapper1 extends Mapper<LongWritable,Text,Text,MapWritable> {
   		private Text userId = new Text();
   		
   		private MultipleOutputs cache;
        public void setup(Context context) {
            cache = new MultipleOutputs(context);
        }

   		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	   		String[] tokens = value.toString().split(",");
	   		String movieName = tokens[0];
	   		
	   		for (int i = 1; i < tokens.length; i++) {
	   			if(tokens[i].length() != 0 ) {
	   				MapWritable mymap = new MapWritable();
	   				userId.set(String.valueOf(i)); 
	   				IntWritable val = new IntWritable(Integer.parseInt(tokens[i]));
	   				mymap.put(new Text(String.valueOf(key.get())), new Text(tokens[i]));
					context.write(userId, mymap); 
		   		} 
			}
			cache.write(CACHE_PATH, new Text(String.valueOf(key.get())), new Text(movieName));   
		}

	    protected void cleanup(Context context) throws IOException, InterruptedException {
	        cache.close();
	    }
   	}

 	public static class Task4Reducer1 extends Reducer<Text,MapWritable, Text,Text> {
    	public void reduce(Text userId, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
			MapWritable map = new MapWritable();
			StringBuilder sb = new StringBuilder();
			boolean isFirst = true;
			for (MapWritable value: values) {
			    for (Map.Entry<Writable,Writable> entry: value.entrySet()) {
				    if (isFirst) {
				    	isFirst = false;
				    } else {
				    	sb.append(",");
				    }
				    sb.append((Text)entry.getKey() + ":" + (Text)entry.getValue());
			    }
			}
			//context.write( NullWritable.get(), new Text(sb.toString()));
			context.write(userId, new Text(sb.toString()));
    	}
   	}



  	public static class Task4Mapper2 extends Mapper<Object,Text,Text,MapWritable> {
   		private Text userId = new Text();
   		private final static IntWritable one = new IntWritable(1);
   		private final static IntWritable zero = new IntWritable(0);

   		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	   		//String[] tokensOriginal = value.toString().split(",");
	   		//String userId = tokensOriginal[0];
			String[] tokenss = value.toString().split(",");
			String[] tokens = Arrays.copyOfRange(tokenss, 1, tokenss.length);

			Arrays.sort(tokens, Comparator.comparing((String arr) -> arr.split(":")[0]));
	   		for (int i = 0; i < tokens.length; i++) {
				MapWritable mymap = new MapWritable();
				String[] first = tokens[i].split(":");
	   			for (int j = i+1; j < tokens.length; j++) {

	   				String[] second = tokens[j].split(":");
	   				if (first[1].equals(second[1])) {
						mymap.put(new Text(second[0]), one);
					} else {
						mymap.put(new Text(second[0]), zero);
					}

	   			}

	   			context.write(new Text(first[0]), mymap);
			}
	   	}
   	}

	public static class Task4Combiner2
        extends Reducer<Text, MapWritable, Text, MapWritable> {
	    private Text pair = new Text();

	    public void reduce(Text word1, Iterable<MapWritable> stripes, 
	                       Context context
	                       ) throws IOException, InterruptedException {
			MapWritable map = new MapWritable();
			for (MapWritable stripe: stripes) {
			    for (Map.Entry<Writable,Writable> entry: stripe.entrySet()) {
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
			context.write(word1, map);
	    }
	}

   	public static class Task4Reducer2 extends Reducer<Text,MapWritable,Text,IntWritable> {
		private Text pair = new Text();
   		private IntWritable result = new IntWritable();
		private static HashMap<String, String> DepartmentMap = new HashMap<String, String>();
		private BufferedReader brReader;
		enum MYCOUNTER {
			RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
		}

		protected void setup(Context context) throws IOException,
				InterruptedException {
			Path[] cacheFilesLocal = context.getLocalCacheFiles();	
			loadDepartmentsHashMap(new Path(context.getConfiguration().get("mapreduce.job.cache.files")), context);
			//for (Path eachPath : cacheFilesLocal) {
			//	System.out.println("got path !!");
			//	if (eachPath.getName().toString().trim().equals("cache-m-00000")) {
			//			context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
			//		loadDepartmentsHashMap(eachPath, context);
			//	}
			//}

		}

		private void loadDepartmentsHashMap(Path filePath, Context context)
				throws IOException {

			String strLineRead = "";

			try {
				brReader = new BufferedReader(new FileReader(filePath.toString()));

				while ((strLineRead = brReader.readLine()) != null) {
					String deptFieldArray[] = strLineRead.split(",");
					DepartmentMap.put(deptFieldArray[0].trim(),
							deptFieldArray[1].trim());
				}
				System.out.println("mapping done");
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);
			} catch (IOException e) {
				context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
				e.printStackTrace();
			}finally {
				if (brReader != null) {
					brReader.close();

				}

			}
		}

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
			    	
			    String wordFirst = DepartmentMap.get(word1.toString());
			    String wordSecond = DepartmentMap.get(word2.toString());
			    
			    pair.set(wordFirst + "," + wordSecond);
			    context.write(pair, (IntWritable)entry.getValue());
			}
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
	    Job job = new Job(conf, "Task 4");
		job.setJar("Task4.jar");

		MultipleOutputs.addNamedOutput(job, new Path(CACHE_PATH).toString(), TextOutputFormat.class, Text.class, Text.class);

	    job.setMapperClass(Task4Mapper1.class);
	    job.setReducerClass(Task4Reducer1.class);

	    job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

	    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    TextOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

	    job.waitForCompletion(true);

	    Job job2 = new Job(conf, "Job 2");

	    job2.addCacheFile(new Path(OUTPUT_PATH+"/cache-m-00000").toUri());


//	    DistributedCache.addCacheFile(new URI("/home/sh33kim/ece454/lab2/intermediate_output/cache-m-00000"), job2.getConfiguration());

		job2.setJar("Task4.jar");

		job2.setMapperClass(Task4Mapper2.class);
		job2.setReducerClass(Task4Reducer2.class);
//		job2.setCombinerClass(Task4Combiner2.class);


		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(MapWritable.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		TextInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
		TextOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
	    
	    System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}

}

