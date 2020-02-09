package commonWords;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class WordCount {
    //Mapper 1
    public static class Mapper1 extends Mapper<Object, Text, Text, CompositeValueWritable> {

        private Text word = new Text();
        private final HashMap<Text, Integer> countMap = new HashMap<Text, Integer>();

        @Override
        protected void setup(Mapper<Object, Text, Text, CompositeValueWritable>.Context context)
                throws IOException, InterruptedException {
            System.out.println("Mapper 1 setup");
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if (countMap.containsKey(word)) {
                    countMap.put(word, countMap.get(word) + 1);
                }else {
                    //needs to create a new Text object as key. Otherwise Hadoop modifies this object.
                    countMap.put(new Text(word), 1);
                }
            }

        }

        @Override
        protected void cleanup(Mapper<Object, Text, Text, CompositeValueWritable>.Context context)
                throws IOException, InterruptedException {
            System.out.println("Mapper 1 cleanup");

            for(Text textKey : countMap.keySet()) {
                //System.out.println("Map 1 = key: "+ textKey +" |val: "+countMap.get(textKey));
                context.write(textKey, new CompositeValueWritable(countMap.get(textKey), 1));
            }
        }
    }

    //Mapper 2
    public static class Mapper2 extends Mapper<Object, Text, Text, CompositeValueWritable> {

        private Text word = new Text();
        private HashMap<Text, Integer> countMap = new HashMap<Text, Integer>();

        @Override
        protected void setup(Mapper<Object, Text, Text, CompositeValueWritable>.Context context)
                throws IOException, InterruptedException {
            System.out.println("Mapper 2 setup");
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if (countMap.containsKey(word)) {
                    countMap.put(word, countMap.get(word) + 1);
                }else {
                    countMap.put(new Text(word), 1);
                }
            }
        }

        @Override
        protected void cleanup(Mapper<Object, Text, Text, CompositeValueWritable>.Context context)
                throws IOException, InterruptedException {
            System.out.println("Mapper 2 cleanup");
            for(Text textKey: countMap.keySet()) {
                //System.out.println("Map 2 = key: "+textKey+" |val: "+countMap.get(textKey));
                context.write(textKey, new CompositeValueWritable(countMap.get(textKey), 2));
            }
        }

    }
    //sum the wordcount
    public static class IntSumReducer extends
            Reducer<Text, CompositeValueWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<CompositeValueWritable> values,
                           Context context) throws IOException, InterruptedException {

            int frequency1 = 0;
            int frequency2 = 0;
            for (CompositeValueWritable val : values) {
                    if (val.getId() == 1) {
                        frequency1 = val.getFrequency();
                    }else if (val.getId() == 2) {
                        frequency2 = val.getFrequency();
                    }
            }
            int min = Math.min(frequency1, frequency2);
            System.out.println("text: "+key+" |frequency1: "+frequency1+" |frequency2: "+frequency2+" |min: "+min);
            if (min > 0) {
                result.set(min);
                context.write(key, result);
            }
        }
    }
    ///////////////////////////////////////////////////////////////////////////////////
    //main function

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        //here you can change another way to get the input and output

        if (otherArgs.length != 3) {
            System.err
                    .println("Usage: wordcountmultipleinputs <input1> <input2> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "word count multiple inputs");
        job.setJarByClass(WordCount.class);

        MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
                TextInputFormat.class, Mapper1.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
                TextInputFormat.class, Mapper2.class);

//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CompositeValueWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
