
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import recommendpkg.Step1.Step1_ToItemPreMapper;
//import recommendpkg.Step1.Step1_ToUserVectorReducer;
//import org.apache.hadoop.examples.HDFSAPI;

public class Step2 {
	public static class Step2_UserVectorToCooccurrenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static Text k = new Text();
        private final static IntWritable v = new IntWritable(1);
        public static final Pattern DELIMITER = Pattern.compile("[:]");

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            System.out.println(values.toString());
            //ToDo
            List<Integer> itemList = new ArrayList<Integer>();
            for (String token:tokens) {
                String[] valueTokens = DELIMITER.split(token);
                assert (valueTokens.length == 2) : "Value token is invalid.";
                int itemId = Integer.parseInt(valueTokens[0]);
                itemList.add(itemId);
            }

            for (int i = 0; i < itemList.size(); i++) {
                for(int j = 0; j < itemList.size(); j++) {
                    int itemId1 = itemList.get(i);
                    int itemId2 = itemList.get(j);
//                    System.out.println(Integer.toString(itemId1) + "_" + Integer.toString(itemId2));
                    k.set(Integer.toString(itemId1) + "_" + Integer.toString(itemId2));
                    v.set(1);
                    context.write(k, v);
                }
            }
        }
    }

    public static class Step2_UserVectorToConoccurrenceReducer extends Reducer <Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            //ToDo
            int sum = 0;
            for(IntWritable value: values) {
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
    	//get configuration info
    	Configuration conf = Recommend.config();
    	//get I/O path
        Path input = new Path(path.get("Step2Input"));
        Path output = new Path(path.get("Step2Output"));
        //delete last saved output
        HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
        hdfs.delFile(output);
        //set job
        Job job =Job.getInstance(conf,"Step2");
		job.setJarByClass(Step2.class);
		
		job.setMapperClass(Step2_UserVectorToCooccurrenceMapper.class);
		job.setCombinerClass(Step2_UserVectorToConoccurrenceReducer.class);
		job.setReducerClass(Step2_UserVectorToConoccurrenceReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job,input);
		FileOutputFormat.setOutputPath(job,output);
		//run job
		job.waitForCompletion(true);
    }
}


