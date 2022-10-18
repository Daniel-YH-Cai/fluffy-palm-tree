//package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class NgramCount {

//    private static class NGCMapper extends Mapper<Object, Text,Text, IntWritable> {
//        private static int N;
//        private static ArrayList<String> lastMapWords;
//
//        @Override
//        protected void setup(Context context) throws IOException, InterruptedException {
//            lastMapWords=new ArrayList<>();
//            N=Integer.parseInt(context.getConfiguration().get("N"));
//        }
//
//        @Override
//        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            String[] tokens=value.toString().split("\\W+");
//            if(tokens.length==0){
//                return;
//            }
//            int startIndex="".equals(tokens[0])?1:0;
//            int cachedSize=lastMapWords.size();
//            int newLength=cachedSize+tokens.length-startIndex;
//            if(newLength<N){
//                lastMapWords.addAll(Arrays.asList(tokens).subList(startIndex, tokens.length));
//                return;
//            }
//            else{
//                for(int i=0;i<lastMapWords.size();i++){
//                    StringBuilder sb=new StringBuilder();
//                    sb.append(lastMapWords.get(i));
//                    for(int j=1;j<=N-1;i++){
//                        sb.append(' ');
//                        if(j<lastMapWords.size()){ ;
//                            sb.append(lastMapWords.get(j));
//                        }
//                        else{
//                            sb.append(tokens[j+startIndex-lastMapWords.size()]);
//                        }
//                    }
//                    context.write(new Text(sb.toString()),new IntWritable(1));
//                }
//                for(int i=startIndex;i<tokens.length-N+1;i++){
//                    StringBuilder sb=new StringBuilder();
//                    sb.append(tokens[i]);
//                    for(int j=1;j<=N-1;j++){
//                        sb.append(' ');
//                        sb.append(tokens[j+i]);
//                    }
//                    context.write(new Text(sb.toString()),new IntWritable(1));
//                }
//                for(int i=lastMapWords.size();i<N-1;i++){
//                    lastMapWords.add("");
//                }
//                for(int i=1;i<=N-1;i++){
//                    int indexInToken=tokens.length-N+i;
//                    if(indexInToken<0){
//                        lastMapWords.set(i-1,lastMapWords.get(lastMapWords.size()+indexInToken));
//                    }
//                    else{
//                        lastMapWords.set(i-1,tokens[indexInToken]);
//                    }
//                }
//
//            }
//
//            //adding the last N-1 words to the deque
//        }
//    }
    private static class NGCMapper extends Mapper<Object, Text,Text, IntWritable>{
    private static final IntWritable ONE=new IntWritable(1);
    @Override
    public void run(Context context) throws IOException, InterruptedException {
        int N=Integer.parseInt(context.getConfiguration().get("N"));
        Deque<String> lastN=new ArrayDeque<>();
        while(context.nextKeyValue()){
            Text value=context.getCurrentValue();
            String[] tokens=value.toString().split("\\W+");
            if(tokens.length==0){
                continue;
            }
            int startIndex="".equals(tokens[0])?1:0;
            while (startIndex<tokens.length&&lastN.size()<N){
                lastN.offerLast(tokens[startIndex]);
                startIndex++;
            }
            for(int i=startIndex;i<tokens.length;i++){
                context.write(toNgram(lastN),ONE);
                lastN.pollFirst();
                lastN.offerLast(tokens[i]);
            }

        }
        context.write(toNgram(lastN),ONE);
    }
    private static Text toNgram(Deque<String> d){
        Iterator<String> itr=d.iterator();
        StringBuilder sb=new StringBuilder();
        if(itr.hasNext()){
            sb.append(itr.next());
        }
        while (itr.hasNext()){
            sb.append(' ');
            sb.append(itr.next());
        }
        return new Text(sb.toString());
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
    private static class NGCTextInputFormat extends TextInputFormat{
        @Override
        protected boolean isSplitable(JobContext context, Path file) {
            return false;
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
        job.setInputFormatClass(NgramCount.NGCTextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
