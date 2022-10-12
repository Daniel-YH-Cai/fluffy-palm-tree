//comment out package as requested in spec
//package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.StringTokenizer;


public class WordLengthCount {
    public static class WLCMapper extends Mapper<Object, Text,IntWritable, IntWritable>{
        @Override
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
            StringTokenizer st=new StringTokenizer(value.toString());
            while (st.hasMoreTokens()){
                String nextToken=st.nextToken();
                context.write(new IntWritable(nextToken.length()),new IntWritable(1));
            }
        }
    }
    public static class WLCReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
            for(IntWritable iw:values){
                    count=count+iw.get();
            }
            context.write(key,new IntWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word length count");
        job.setJarByClass(WordLengthCount.class);
        job.setMapperClass(WordLengthCount.WLCMapper.class);
        job.setCombinerClass(WordLengthCount.WLCReducer.class);
        job.setReducerClass(WordLengthCount.WLCReducer.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
