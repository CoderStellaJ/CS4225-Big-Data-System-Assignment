
import java.io.IOException;
import java.util.HashMap;
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
            //System.out.println(values);
//            System.out.println("values in step 4_1 map: "+values);
            String[] tokens = DELIMITER.split(values.toString());
            String itemIdPair = tokens[0];
            String score_frequency = tokens[1];
            k.set(itemIdPair);
            v.set(score_frequency);
            context.write(k,v);
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
            //<item1_item2_userId, score> and <item1_item2_userId, freq> -> <item1_item2_userId, mul>
            int count = 0;
            float mul = 1;
            for(Text val: values) {
                count++;
                mul *= Float.parseFloat(val.toString());
            }
            if(count == 2) {
                k.set(key);
                v.set(Float.toString(mul));
                context.write(k, v);
            }
//            float frequency = 0;
//            String key_string = key.toString();
//
//            for(Text value : values) {
//                if(!value.toString().contains(UNDERSCORE)) {
//                    frequency = Float.parseFloat(value.toString());
//                }
//            }
//            //System.out.println("frequency: "+frequency);
//            for(Text value : values) {
//                String valueString = value.toString();
//                System.out.println("outside if, valueString is " + valueString);
//                if(valueString.contains(UNDERSCORE)) {
//                    System.out.println("inside if");
//                    String[] tokens = DELIMITER_UNDERSCORE.split(valueString);
//                    String userId = tokens[0];
//                    String score = tokens[1];
//                    float score_num = Float.parseFloat(score);
//                    System.out.println("context write: "+ key_string + UNDERSCORE + userId);
//                    k.set(key_string + UNDERSCORE + userId);
//                    v.set(Float.toString(score_num * frequency));
//                    context.write(k, v);
//                }
//            }

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

