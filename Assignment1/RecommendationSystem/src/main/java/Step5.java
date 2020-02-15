import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//
//import HDFSAPI;

public class Step5 {
	public static class Step5_FilterSortMapper extends Mapper<LongWritable, Text, Text, Text> {
		private String flag;
		private final Text k = new Text();
		private final Text v = new Text();
		public static final Pattern DELIMITER = Pattern.compile("[\t]");
		public static final Pattern DELIMITER_UNDERSCORE = Pattern.compile("[_]");
		//E0134062 (62)
		public static final int USERID = 62;

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
//			System.out.println("mapper 5 setup");
			FileSplit split = (FileSplit) context.getInputSplit();
			flag = split.getPath().getParent().getName();// dataset
		}

		@Override
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			//you can use provided SortHashMap.java or design your own code.
			//ToDo
			//sort the items based on the score
			String[] tokens = DELIMITER.split(values.toString());
			String userItem = tokens[0];
			String score = tokens[1];
			String[] keyTokens = DELIMITER_UNDERSCORE.split(userItem);
			String itemId = keyTokens[0];
			String userId = keyTokens[1];

			if(Integer.parseInt(userId) == USERID) {
				k.set(itemId);
				v.set(score);
				context.write(k, v);
			}

		}
	}


	public static class Step5_FilterSortReducer extends Reducer<Text, Text, Text, Text> {
		private final Text k = new Text();
		private final Text v = new Text();
		private final HashMap<String, Float> resultMap = new HashMap<String, Float>();
		private final SortHashMap sortMap = new SortHashMap();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	//you can use provided SortHashMap.java or design your own code.
            //ToDo
			int k = 0;
			for(Text value: values) {
				k++;
				resultMap.put(key.toString(), Float.parseFloat(value.toString()));
			}
			if(k >= 2) {
				System.out.println("error in step 5 reducer");
			}
        }

		@Override
		public void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			List<Entry<String,Float>> result = sortMap.sortHashMap(resultMap);
//			System.out.println("in step 5");
			for(Entry<String, Float> entry: result) {
//				System.out.println(entry.getKey() + "\t" + entry.getValue());
				k.set(entry.getKey());
				v.set(entry.getValue().toString());
				context.write(k, v);
			}
		}
    }

	public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
		//get configuration info
		Configuration conf = Recommend.config();
		// I/O path
		Path input1 = new Path(path.get("Step5Input1"));
		// Path input2 = new Path(path.get("Step5Input2"));
		Path output = new Path(path.get("Step5Output"));
		// delete last saved output
		HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
		hdfs.delFile(output);
		// set job
        Job job =Job.getInstance(conf);
        job.setJarByClass(Step5.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step5_FilterSortMapper.class);
        job.setReducerClass(Step5_FilterSortReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // FileInputFormat.setInputPaths(job, input1,input2);
		FileInputFormat.setInputPaths(job, input1);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
	}
}

