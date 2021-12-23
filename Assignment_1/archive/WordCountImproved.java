import java.io.IOException;
import java.util.StringTokenizer; //not in use
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;//not in use
import org.apache.hadoop.io.LongWritable;
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

public class WordCountImproved extends Configured implements Tool {

	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongWritable>{ //changed IntWritable to LongWritable

		private final static LongWritable one = new LongWritable(1);
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
			
			/* Start of a new context for the word count 14Sep */
			//convert word from text to string
			String word_str = value.toString();
	
	    	//ignores punctuation by splitting regular expression
	    	Pattern word_sep = Pattern.compile("[\\p{Punct}\\s]+");
	    	String[] words = word_sep.split(word_str);

	    	//set each word as key to the key-value pair
	    	for(String word : words) {
	    		//ignore case & trim the word
	    		Text wordResult = new Text(word.toLowerCase().trim());
	    		context.write(wordResult, one);
	    	}
	    	/* End of a new context for the word count 14Sep */  
		    
		}
	}

/*	public static class IntSumReducer
	extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
*/
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCount(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(LongSumReducer.class); //removed InSumReducer and changed to LongSumReducer, error
		job.setReducerClass(LongSumReducer.class); //removed InSumReducer and changed to LongSumReducer, error

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class); // changed from IntWritable to LongWritable
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}