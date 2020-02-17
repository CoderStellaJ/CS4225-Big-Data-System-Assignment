
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//
//import HDFSAPI;

public class Step4_1 {
	public static class Step4_PartialMultiplyMapper extends Mapper<LongWritable, Text, Text, Text> {
        private String flag;
	    private final static Text k = new Text();
        private final static Text v = new Text();
        public static final Pattern DELIMITER = Pattern.compile("[\t]");

        // you can solve the co-occurrence Matrix/left matrix and score matrix/right matrix separately

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();// data set
        }

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            //ToDo
            //<itemId1_itemId2, user_score/frequency> -> <itemId1_itemId2, user_score/frequency>

            String[] tokens = DELIMITER.split(values.toString());
            String itemIdPair = tokens[0];
            String score_frequency = tokens[1];

            k.set(itemIdPair);
            v.set(score_frequency);
            context.write(k, v);

        }

    }

    public static class Step4_AggregateReducer extends Reducer<Text, Text, Text, Text> {
        private final static Text k = new Text();
        private final static Text v = new Text();
        public static final Pattern DELIMITER_UNDERSCORE = Pattern.compile("[\t_]");
        public static final String UNDERSCORE = "_";

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //ToDo
            //multiply the values with the same key
            //<item1_item2, score> and <item1_item2, freq> -> <item1_item2_userId, mul>
            HashSet<String> userScoreSet = new HashSet<String>();

            float frequency = 0;
            for(Text val: values) {
                String valString = val.toString();
                if(valString.contains(UNDERSCORE)) {
                    userScoreSet.add(valString);
                }else {
                    frequency = Float.parseFloat(valString);
                }
            }

            for(String userScore: userScoreSet) {
                String[] userScoreTokens = DELIMITER_UNDERSCORE.split(userScore);
                String userId = userScoreTokens[0];
                float score = Float.parseFloat(userScoreTokens[1]);
                k.set(key + UNDERSCORE + userId);
                v.set(Float.toString(score * frequency));
                context.write(k, v);
            }

        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
    	//get configuration info
		Configuration conf = Recommend.config();
		// get I/O path
		Path input1 = new Path(path.get("Step4_1Input1"));
		Path input2 = new Path(path.get("Step4_1Input2"));
		Path output = new Path(path.get("Step4_1Output"));
		// delete last saved output
		HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
		hdfs.delFile(output);
		// set job
        Job job =Job.getInstance(conf);
        job.setJarByClass(Step4_1.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step4_1.Step4_PartialMultiplyMapper.class);
        job.setReducerClass(Step4_1.Step4_AggregateReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, input1, input2);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
    }
}

