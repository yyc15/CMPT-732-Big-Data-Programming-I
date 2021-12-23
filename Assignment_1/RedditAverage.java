import java.io.IOException;
import java.util.StringTokenizer; //not in use
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;//not in use
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;//not in use
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer; //import longSumReducer
import org.json.JSONObject;//import JSON Package

public class RedditAverage extends Configured implements Tool {

	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongPairWritable>{ //changed IntWritable to LongWritable

		
		private final static long one = 1;
		private final static LongPairWritable pair = new LongPairWritable();
		//private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			 /* original using StringTokenizer
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one); //produce key value pair
			}*/
			
			//convert word from text to string
			String input_str = value.toString();
			//ignore case
			input_str = input_str.toLowerCase();
			
			//trim the word_str

		    	JSONObject record = new JSONObject(input_str);
		    	
				System.out.println((String) record.get("subreddit"));
				System.out.println((Integer) record.get("score"));
				
		    	String subreddit =(String) record.get("subreddit");
		    	Integer score = (Integer) record.get("score");
		    	
				pair.set(one, score);
				context.write(new Text(subreddit), pair);
				
		    	/* -------------------From WordCount-------------------
		    	//ignores punctuation by splitting regular expression
		    	Pattern word_sep = Pattern.compile("[\\p{Punct}\\s]+");
		    	String[] words = word_sep.split(input_str);

		        // set each word as key to the key-value pair
		        for(String word : words)
		        	context.write(new Text(word), one);
		        ------------------------------------------------------*/
		    
		}
	}
	
	


	public static class RedditReducer
		extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
			
			Double redditSum = 0.0;
			long redditCount = 0;
			
			for (LongPairWritable val : values) {
				
				redditCount += val.get_0();
				redditSum += val.get_1();
				
			}
			result.set(redditSum/redditCount); //calculates the average for each subreddit
			context.write(key, result);
		}
	}


    public static class RedditCombiner
    extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
            private LongPairWritable result = new LongPairWritable();

           @Override
            public void reduce(Text key, Iterable<LongPairWritable> values,
                            Context context
                            ) throws IOException, InterruptedException {
                    long redditSum = 0;
              		long redditCount =0;
                    for (LongPairWritable val : values) {
                    	redditCount += val.get_0();
                    	redditSum += val.get_1(); 
                    }
                    result.set(redditCount, redditSum);
                    context.write(key, result);
            }
    }



	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "reddit average");
		job.setJarByClass(RedditAverage.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(RedditCombiner.class); //change to combiner class name
		job.setReducerClass(RedditReducer.class); //change to reducer class name

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongPairWritable.class); //changed to LongPairWritable
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}