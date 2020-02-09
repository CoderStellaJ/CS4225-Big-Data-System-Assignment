package commonWords;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
        private final Set<String> stopwords = new HashSet<String>();
        private static final String STOP_WORD_PATH = "C:\\Repository\\CS4225-Data-System\\Assignment1\\Task1_data\\stopwords.txt";

        @Override
        protected void setup(Mapper<Object, Text, Text, CompositeValueWritable>.Context context)
                throws IOException, InterruptedException {
            System.out.println("Mapper 1 setup");
            Configuration conf = context.getConfiguration();
            try {
                Path path = new Path(STOP_WORD_PATH);
                FileSystem fs= FileSystem.get(new Configuration());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                String word = null;
                while ((word= br.readLine())!= null) {
                    this.stopwords.add(word.toLowerCase());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if(!stopwords.contains(word.toString().toLowerCase())) {
                    if (countMap.containsKey(word)) {
                        countMap.put(word, countMap.get(word) + 1);
                    }else {
                        //needs to create a new Text object as key. Otherwise Hadoop modifies this object.
                        countMap.put(new Text(word), 1);
                    }
                }
            }

        }

        @Override
        protected void cleanup(Mapper<Object, Text, Text, CompositeValueWritable>.Context context)
                throws IOException, InterruptedException {
            System.out.println("Mapper 1 cleanup");

            for(Text textKey : countMap.keySet()) {
                context.write(textKey, new CompositeValueWritable(countMap.get(textKey), 1));
            }
        }
    }

    //Mapper 2
    public static class Mapper2 extends Mapper<Object, Text, Text, CompositeValueWritable> {

        private Text word = new Text();
        private HashMap<Text, Integer> countMap = new HashMap<Text, Integer>();
        private final Set<String> stopwords = new HashSet<String>();
        private static final String STOP_WORD_PATH = "C:\\Repository\\CS4225-Data-System\\Assignment1\\Task1_data\\stopwords.txt";

        @Override
        protected void setup(Mapper<Object, Text, Text, CompositeValueWritable>.Context context)
                throws IOException, InterruptedException {
            System.out.println("Mapper 2 setup");
            Configuration conf = context.getConfiguration();
            try {
                Path path = new Path(STOP_WORD_PATH);
                FileSystem fs= FileSystem.get(new Configuration());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                String word = null;
                while ((word= br.readLine())!= null) {
                    this.stopwords.add(word.toLowerCase());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if(!stopwords.contains(word.toString().toLowerCase())) {
                    if (countMap.containsKey(word)) {
                        countMap.put(word, countMap.get(word) + 1);
                    }else {
                        countMap.put(new Text(word), 1);
                    }
                }
            }
        }

        @Override
        protected void cleanup(Mapper<Object, Text, Text, CompositeValueWritable>.Context context)
                throws IOException, InterruptedException {
            System.out.println("Mapper 2 cleanup");
            for(Text textKey: countMap.keySet()) {
                context.write(textKey, new CompositeValueWritable(countMap.get(textKey), 2));
            }
        }

    }
    //sum the wordcount
    public static class IntSumReducer extends
            Reducer<Text, CompositeValueWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private final HashMap<Text, Integer> resultMap = new HashMap<Text, Integer>();
        public static final int NUM = 15;

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
                    resultMap.put(new Text(key), min);
            }
        }

        @Override
        public void cleanup(Reducer<Text, CompositeValueWritable, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            //sort the resultmap based on value
            Comparator<Map.Entry<Text, Integer>> valueComparator = new Comparator<Map.Entry<Text,Integer>>() {
                public int compare(Map.Entry<Text, Integer> e1, Map.Entry<Text, Integer> e2) {
                    Integer v1 = e1.getValue();
                    Integer v2 = e2.getValue();
                    //descending order
                    return v2.compareTo(v1);
                }
            };
            List<Map.Entry<Text, Integer>> listOfEntries = new ArrayList<Map.Entry<Text, Integer>>(resultMap.entrySet());
            Collections.sort(listOfEntries, valueComparator);
            int k = 0;
            for(Map.Entry<Text, Integer> entry: listOfEntries) {
                k++;
                result.set(entry.getValue());
                context.write(entry.getKey(), result);
                if(k >= NUM) {
                    break;
                }
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
