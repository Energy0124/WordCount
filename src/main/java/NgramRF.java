import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.StringJoiner;

public class NgramRF {

    public static class NgramRFMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        Text ngram = new Text();
        Text ngramSum = new Text();
        //        IntWritable count = new IntWritable(1);
        IntWritable one = new IntWritable(1);
        List<String> list;
        int n;
        double theta;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] tokens = value.toString().replaceFirst("^[\\p{Punct}\\s]+", "").split("[\\p{Punct}\\s]+");


            for (int i = 0; i < tokens.length; i++) {
                if (!tokens[i].equals("")) {
                    list.add(tokens[i]); //add to queue if not empty
                    if (list.size() >= n) { //if longer than n, start to emit
                        StringJoiner stringJoiner = new StringJoiner(" ");
                        for (int j = 0; j < n; j++) {
                            stringJoiner.add(list.get(j));
                        }
                        ngram.set(stringJoiner.toString());
                        context.write(ngram, one);// emit the ngram count

                        ngramSum.set(list.get(0) + " *");
                        context.write(ngramSum, one);//emit the sum
                        list.remove(0); //remove first in queue
                    }
                }


            }
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            n = conf.getInt("N", 1);
            theta = conf.getFloat("theta", 0);
            list = new LinkedList<String>();
        }
//
//        protected void cleanup(Context context) throws IOException, InterruptedException {
//            for (Map.Entry<IntWritable, IntWritable> entry : inmemMap.entrySet()) {
//                System.out.println(entry.getKey() + " : " + entry.getValue());
//                context.write(entry.getKey(), entry.getValue());
//            }
//        }
    }

    public static class NgramRFReducer
            extends Reducer<Text, IntWritable, Text, FloatWritable> {
        private FloatWritable result = new FloatWritable();
        int ngramSum = 0;
        int n;
        float theta;
        String ngramFirstWord = "";

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            n = conf.getInt("N", 1);
            theta = conf.getFloat("theta", 0);
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String[] tokens = key.toString().split("[\\s]+");
            if (tokens.length == 2 && tokens[1].equals("*")) {
                ngramFirstWord = tokens[0];
                ngramSum = 0;
                for (IntWritable val : values) {
                    ngramSum += val.get();
                }
                System.out.println(ngramFirstWord+": "+ngramSum);
            } else {

                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                float rf =sum / ngramSum;
                if (rf >= theta) {
                    result.set(rf);
                    context.write(key, result);
                }

            }


        }
    }

    public static class WordPartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String[] tokens = key.toString().split("[\\s]+");
            for (String token : tokens) {
                if (!token.equals("")) {
                    return (token.hashCode() & Integer.MAX_VALUE) % numReduceTasks; //return hash of first non empty token
                }
            }

            return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks; //error, just return the full hash

        }


    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("N", Integer.parseInt(args[2]));
        conf.setFloat("theta", Float.parseFloat(args[3]));

        Job job = Job.getInstance(conf, "ngram count");
        job.setJarByClass(NgramRF.class);
        job.setMapperClass(NgramRFMapper.class);
        job.setCombinerClass(NgramRFReducer.class);
        job.setReducerClass(NgramRFReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setPartitionerClass(WordPartitioner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
