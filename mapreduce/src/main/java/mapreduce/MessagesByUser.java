package mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


public class MessagesByUser {

	public static class MessagesByUserMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String json = value.toString();
			String[] line = json.split("\\n");
			String screen_name;
			String tweet;

			try {

				for(int i=0;i<line.length; i++){
					JSONObject obj = new JSONObject(line[i]);
					screen_name = (String) ((JSONObject) obj.get("user")).get("screen_name");
					tweet = (String) ((JSONObject) obj.get("extended_tweet")).get("full_text");
					context.write(new Text(screen_name), new Text(tweet));
				}
			}catch(JSONException e){
				e.printStackTrace();
			}

		}
	}

	public static class MessagesByUserReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			try{
				JSONObject obj = new JSONObject();
				JSONArray ja = new JSONArray();
				
				for(Text val : values){
					JSONObject jo = new JSONObject().put("tweet", val.toString());
					ja.put(jo);
				}
				obj.put("tweets", ja);
				context.write(key, new Text(obj.toString()));
				
			}catch(JSONException e){
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(MessagesByUser.class);
		job.setMapperClass(MessagesByUserMapper.class);
		//job.setCombinerClass(ScreenNamesReducer.class);
		job.setReducerClass(MessagesByUserReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
