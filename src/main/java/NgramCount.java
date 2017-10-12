import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NgramCount {

    public static class NgramMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        Text ngram = new Text();
        IntWritable count = new IntWritable(1);
        List<String> list;
        int n;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] tokens = value.toString().replaceFirst("^[\\p{Punct}\\s]+", "").split("[\\p{Punct}\\s]+");


            for (int i = 0; i < tokens.length; i++) {
                if (!tokens[i].equals("")) {
                    list.add(tokens[i]);
                    if (list.size() >= n) {
                        StringJoiner stringJoiner = new StringJoiner(" ");
                        for (int j = 0; j < n; j++) {
                            stringJoiner.add(list.get(j));
                        }
                        ngram.set(stringJoiner.toString());
                        context.write(ngram, count);
                        list.remove(0);
                    }
                }


            }
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            n = conf.getInt("N", 1);
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

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("N", Integer.parseInt(args[2]));

        Job job = Job.getInstance(conf, "ngram count");
        job.setJarByClass(NgramCount.class);
        job.setMapperClass(NgramMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
