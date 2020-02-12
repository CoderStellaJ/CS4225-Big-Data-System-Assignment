
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import org.apache.hadoop.examples.HDFSAPI;

public class Step3 {
	public static class Step31_UserVectorSplitterMapper extends Mapper<IntWritable, Text, Text, Text> {
		private final static Text k = new Text();
		private final static Text v = new Text();
		//E0134062 (user 62)
		private final static int USER_ID = 62;
		public static final Pattern DELIMITER_COMMA = Pattern.compile("[\t,]");
		public static final Pattern DELIMITER_COLON = Pattern.compile("[:]");

		@Override
		public void map(IntWritable key, Text values, Context context)
				throws IOException, InterruptedException {
			//ToDo
			//<itemId, score> -> <itemId1_itemId2, score>
			if(key.get() == USER_ID) {
				List<String> itemList = new ArrayList<String>();
				Map<String, String> scoreMap = new HashMap<String, String>();
				String[] tokens = DELIMITER_COMMA.split(values.toString());
				for(String token: tokens) {
					String[] elements = DELIMITER_COLON.split(token);
					String itemId = elements[0];
					String score = elements[1];
					itemList.add(itemId);
					scoreMap.put(itemId, score);
				}
				for(int i = 0; i < itemList.size(); i++) {
					for(int j = 0; j < itemList.size(); j++) {
						String itemId1 = itemList.get(i);
						String itemId2 = itemList.get(j);
						String score = scoreMap.get(itemId1);
						k.set(itemId1 + "_" + itemId2);
						v.set(score);
						context.write(k, v);
					}
				}
			}
		}
	}

	public static void run1(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
		//get configuration info
    	Configuration conf = Recommend.config();
    	//get I/O path
        Path input = new Path(path.get("Step3Input1"));
        Path output = new Path(path.get("Step3Output1"));
        //delete the last saved output
        HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
        hdfs.delFile(output);
        //set job
        Job job =Job.getInstance(conf,"Step3_1");
		job.setJarByClass(Step3.class);
		
		job.setMapperClass(Step31_UserVectorSplitterMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,input);
		FileOutputFormat.setOutputPath(job,output);
		//run job
		job.waitForCompletion(true);
		}
	
	 public static class Step32_CooccurrenceColumnWrapperMapper extends Mapper<Text, IntWritable, Text, Text> {
		 private final static Text k = new Text();
		 private final static Text v = new Text();
		 public static final Pattern DELIMITER_UNDERSCORE = Pattern.compile("[_]");

		@Override
		public void map(Text key, IntWritable values,Context context)
				throws IOException, InterruptedException {
				//ToDo
			    // <itemid1_itemid2, count> -> <itemid1_itemid2, count>
				k.set(key.toString());
				v.set(Integer.toString(values.get()));
				context.write(k, v);
		}
	}

	public static void run2(Map<String, String> path) throws IOException,
		ClassNotFoundException, InterruptedException {
		//get configuration info
		Configuration conf = Recommend.config();
		//get I/O path
		Path input = new Path(path.get("Step3Input2"));
		Path output = new Path(path.get("Step3Output2"));
		// delete the last saved output 
		HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
		hdfs.delFile(output);
		// set job
		Job job =Job.getInstance(conf, "Step3_2");
		job.setJarByClass(Step3.class);

		job.setMapperClass(Step32_CooccurrenceColumnWrapperMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		// run job
		job.waitForCompletion(true);
	}
}

