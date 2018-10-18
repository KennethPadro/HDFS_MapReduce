package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


public class DiseaseCount {

	public static class DiseaseCountMapper extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private final static String[] inputArr = { "trump", "flu", "zika", "diarrhea", "ebola", "headache", "measles"};


		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String json = value.toString();
			String[] line = json.split("\\n");
			StringTokenizer tokenizer_input;
			String tweet;
			
			try {
				
				for(int i=0;i<line.length; i++){
					JSONObject obj = new JSONObject(line[i]);
					tweet = (String) ((JSONObject) obj.get("extended_tweet")).get("full_text");
					tweet = tweet.replaceAll("[^a-zA-Z]", " ");
					tokenizer_input = new StringTokenizer(tweet,"\n \t \r \f : - ? !");
					//tokenizer_input = new StringTokenizer(tweet," ?!.1234567890#()@'$%^&*-_");
					String temp;
					while (tokenizer_input.hasMoreTokens()) {
						temp=tokenizer_input.nextToken();
						for (int i1=0;i1<inputArr.length;i1++){
							if (temp.equalsIgnoreCase(inputArr[i1])){
								word.set(inputArr[i1]);
								context.write(word, one);
							}
						}
					}
				}
			}catch(JSONException e){
				e.printStackTrace();
			}
		}
	}


	public static class DiseaseCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
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
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(DiseaseCount.class);
		job.setMapperClass(DiseaseCountMapper.class);
		job.setCombinerClass(DiseaseCountReducer.class);
		job.setReducerClass(DiseaseCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
