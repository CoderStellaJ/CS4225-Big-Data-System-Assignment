
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import org.apache.hadoop.examples.HDFSAPI;

public class Step3 {
	public static class Step31_UserVectorSplitterMapper extends Mapper<LongWritable, Text, Text, Text> {
		private final static Text k = new Text();
		private final static Text v = new Text();

		//itemId -> userId_score
		public final HashMap<String, ArrayList<String>> itemScoreMap = new HashMap<String, ArrayList<String>>();
		//all items
		public final HashSet<String> itemSet = new HashSet<String>();

		public static final Pattern DELIMITER_COMMA = Pattern.compile("[\t,]");
		public static final Pattern DELIMITER_COLON = Pattern.compile("[:]");
		public static final Pattern DELIMITER_UNDERSCORE = Pattern.compile("[_]");

		@Override
		public void map(LongWritable key, Text values, Context context)
				throws IOException, InterruptedException {
			String[] tokens = DELIMITER_COMMA.split(values.toString());
			String userId = tokens[0];
			for(int i = 1; i < tokens.length; i++) {
				String token = tokens[i];
				String[] elements = DELIMITER_COLON.split(token);
				String itemId = elements[0];
				String score = elements[1];
				itemSet.add(itemId);
				if(!itemScoreMap.containsKey(itemId)) {
					itemScoreMap.put(itemId, new ArrayList<String>());
				}
				ArrayList<String> userScore = itemScoreMap.get(itemId);
				userScore.add(userId+"_"+score);
			}

		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for (Map.Entry<String, ArrayList<String>> entry : itemScoreMap.entrySet()) {
				String itemId1 = entry.getKey();
				ArrayList<String> userScoreList = entry.getValue();
				for(String itemId2: itemSet) {
					for(String userScore: userScoreList) {
						String[] tokens = DELIMITER_UNDERSCORE.split(userScore);
						String userId = tokens[0];
						String score = tokens[1];
						k.set(itemId1+"_"+itemId2 + "_" + userId);
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


//	public static class Step32_CooccurrenceColumnWrapperMapper extends Mapper<LongWritable, Text, Text, Text> {
//		private final static Text k = new Text();
//		private final static Text v = new Text();
//		public static final Pattern DELIMITER = Pattern.compile("[\t]");
//
//		@Override
//		public void map(LongWritable key, Text values,Context context) throws IOException, InterruptedException {
//			//ToDo
//			// <itemid1_itemid2, count> -> <itemid1_itemid2, count>
//			//System.out.println(values);
//			String[] tokens = DELIMITER.split(values.toString());
//			k.set(tokens[0]);
//			v.set(tokens[1]);
//			context.write(k, v);
//		}
//	}
//
//	public static void run2(Map<String, String> path) throws IOException,
//		ClassNotFoundException, InterruptedException {
//		//get configuration info
//		Configuration conf = Recommend.config();
//		//get I/O path
//		Path input = new Path(path.get("Step3Input2"));
//		Path output = new Path(path.get("Step3Output2"));
//		// delete the last saved output
//		HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
//		hdfs.delFile(output);
//		// set job
//		Job job =Job.getInstance(conf, "Step3_2");
//		job.setJarByClass(Step3.class);
//
//		job.setMapperClass(Step32_CooccurrenceColumnWrapperMapper.class);
//
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//
//		FileInputFormat.addInputPath(job, input);
//		FileOutputFormat.setOutputPath(job, output);
//		// run job
//		job.waitForCompletion(true);
//	}

}

