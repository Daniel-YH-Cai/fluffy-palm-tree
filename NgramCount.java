//package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NgramCount {
    private static class NGCMapper extends Mapper<Object, Text,Text, IntWritable> {
        private static int N;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            N=Integer.parseInt(context.getConfiguration().get("N"));
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens=value.toString().split("[\\W]+");
            int startIndex="".equals(tokens[0])?1:0;
            for(int i=startIndex;i<=tokens.length-N;i++){
                StringBuilder sb=new StringBuilder();
                for(int j=i;j<=i+N-1;j++){
                    sb.append(tokens[j]);
                    if(j<i+N-1){
                        sb.append(" ");
                    }
                }
                context.write(new Text(sb.toString()),new IntWritable(1));
            }
        }
    }
    private static class NGCReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable iw:values){
                sum=sum+iw.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        if(Integer.parseInt(args[2])<1){
            throw new IllegalArgumentException("N-gram length must be at least 1");
        }
        Configuration conf=new Configuration();
        conf.set("N",args[2]);
        Job job=Job.getInstance(conf,"n-gram count");
        job.setJarByClass(NgramCount.class);
        job.setMapperClass(NgramCount.NGCMapper.class);
        job.setReducerClass(NgramCount.NGCReducer.class);
        job.setCombinerClass(NgramCount.NGCReducer.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
