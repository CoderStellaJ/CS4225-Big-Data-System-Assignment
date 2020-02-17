
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.hash.Hash;

//import recommendpkg.Step1.Step1_ToItemPreMapper;
//import recommendpkg.Step1.Step1_ToUserVectorReducer;
//import org.apache.hadoop.examples.HDFSAPI;

public class Step2 {
	public static class Step2_UserVectorToCooccurrenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static Text k = new Text();
        private final static IntWritable v = new IntWritable(1);
        public static final Pattern DELIMITER_COLON = Pattern.compile("[\t:]");

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            //ToDo
            //<userid itemid:score, itemid:score> -> <itemid1_itemid2, 1>
            String userId = tokens[0];
            for (int i = 1; i < tokens.length; i++) {
                for (int j = 1; j < tokens.length; j++) {
                    String token1 = tokens[i];
                    String token2 = tokens[j];
                    String[] valueTokens1 = DELIMITER_COLON.split(token1);
                    String[] valueTokens2 = DELIMITER_COLON.split(token2);
                    String itemId1 = valueTokens1[0];
                    String itemId2 = valueTokens2[0];
                    k.set(itemId1 + "_" + itemId2);
                    v.set(1);
                    context.write(k,v);
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
            // <itemid1_itemid2, 1> -> <itemid1_itemid2, count>
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


