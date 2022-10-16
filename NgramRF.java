//package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

//Pair approach
public class NgramRF {
    private static final String MARGINAL_SIGN="$$$";

    private static class NRFMapper extends Mapper<Object, Text, WordArrayWritable,IntWritable> {
        private static final HashMap<String,Integer> marginalCounts=new HashMap<>();
        private static int N=0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            N=Integer.parseInt(context.getConfiguration().get("N"));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Map.Entry<String,Integer> e:marginalCounts.entrySet()){
                WordArrayWritable ngramMargin=new WordArrayWritable();
                ngramMargin.set(new Writable[]{new Text(e.getKey()),new Text(MARGINAL_SIGN)});
                context.write(ngramMargin,new IntWritable(e.getValue()));
            }
        }

        @Override
        //startIndex: If value starts with a non alphanumeric sequence, it might has a empty at the front after spliting
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if(N==1){
                String[] tokens=value.toString().split("[\\W]+");
                if(tokens.length==0){
                    return;
                }
                int startIndex="".equals(tokens[0])?1:0;
                for(int i=startIndex;i<tokens.length;i++){
                    WordArrayWritable tmp=new WordArrayWritable();
                    Writable[] w=new Writable[1];
                    w[0]=new Text(tokens[i]);
                    tmp.set(w);
                    context.write(tmp,new IntWritable(1));
                }
            }
            else{
                String[] tokens=value.toString().split("[\\W]+");
                if(tokens.length==0){
                    return;
                }
                int startIndex="".equals(tokens[0])?1:0;
                for(int i=startIndex;i<=tokens.length-N;i++){
                    Text[] ngram=new Text[N];
                    for(int j=i;j<=i+N-1;j++) {
                        ngram[j-i]=new Text();
                        ngram[j-i].set(tokens[j]);
                    }
                    WordArrayWritable waw=new WordArrayWritable();
                    waw.set(ngram);
                    context.write(waw,new IntWritable(1));
                    if(marginalCounts.containsKey(tokens[i])){
                        marginalCounts.replace(tokens[i],marginalCounts.get(tokens[i])+1);
                    }
                    else{
                        marginalCounts.put(tokens[i],1);
                    }

                }
            }
        }
    }
    private static class DummyReducer extends Reducer<WordArrayWritable,IntWritable,WordArrayWritable,DoubleWritable>{
        @Override
        protected void reduce(WordArrayWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable value:values){
                sum=sum+value.get();
            }
            context.write(key,new DoubleWritable(0.0+sum));
        }
    }

    private static class NRFReducer extends Reducer<WordArrayWritable,IntWritable,WordArrayWritable,DoubleWritable> {
        private static final HashMap<String,Integer> marginalCounts=new HashMap<>();
        private static int N;
        private static double THETA;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            N=Integer.parseInt(context.getConfiguration().get("N"));
            THETA=Double.parseDouble(context.getConfiguration().get("THETA"));
        }

        @Override
        protected void reduce(WordArrayWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Writable[] ngram=key.get();
            int totalValues=0;
            for(IntWritable iw:values){
                totalValues=totalValues+iw.get();
            }
            if(N==1){
                context.write(key,new DoubleWritable(1));
            }
            else{
                if(MARGINAL_SIGN.equals(ngram[1].toString())){
                    marginalCounts.put(ngram[0].toString(),totalValues);
                }
                else{
                    double ratio=((double)totalValues)/(marginalCounts.get(ngram[0].toString()));
                    if(THETA<=ratio){
                        context.write(key,new DoubleWritable(ratio));
                    }
                }
            }
        }
    }
    private static class NRFCombiner extends Reducer<WordArrayWritable, IntWritable, WordArrayWritable, IntWritable> {
        @Override
        protected void reduce(WordArrayWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable iw:values){
                sum=sum+iw.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }
    private static class WordArrayWritable extends ArrayWritable implements WritableComparable<ArrayWritable>{

        public WordArrayWritable() {
            super(Text.class);
        }
        @Override
        public int compareTo(ArrayWritable o) {
            Writable[] myVal=get();
            Writable[] yourVal=o.get();
            //invalid
            if(myVal.length!=yourVal.length){
                System.out.println("[Error] N-gram sequence length do not equal");
            }
            //1 gram
            if(myVal.length==1&&yourVal.length==1){
                return ((Text)myVal[0]).toString().compareTo(((Text)yourVal[0]).toString());
            }
            //both marginal count: compare first word
            if(MARGINAL_SIGN.equals(((Text)myVal[1]).toString())&&MARGINAL_SIGN.equals(((Text)yourVal[1]).toString())){
                return ((Text)myVal[0]).toString().compareTo(((Text)yourVal[0]).toString());
            }
            else if(MARGINAL_SIGN.equals(((Text)myVal[1]).toString())){
                return -1;
            }
            else if(MARGINAL_SIGN.equals(((Text)yourVal[1]).toString())){
                return 1;
            }
            for(int i=0;i<myVal.length;i++){
                int result=((Text)myVal[i]).toString().compareTo(((Text)yourVal[i]).toString());
                if(result!=0){
                    return result;
                }
            }
            return 0;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            Writable[] content=get();
            for(int i=0;i<content.length;i++){
                sb.append(((Text)content[i]).toString());
                if(i<=content.length-2) {
                    sb.append(" ");
                }
            }
            return sb.toString();
        }
    }
    private static class WordArrayPartitioner extends Partitioner<WordArrayWritable,IntWritable>{

        @Override
        //determine reducer using the hashcode of the first word in the ngram
        public int getPartition(WordArrayWritable wordArrayWritable, IntWritable intWritable, int numOfReducer) {
            String ngramStart= wordArrayWritable.get()[0].toString();
            return ngramStart.hashCode()%numOfReducer;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if(Integer.parseInt(args[2])<1){
            throw new IllegalArgumentException("N-gram length must be at least 1");
        }
        Configuration conf=new Configuration();
        conf.set("N",args[2]);
        conf.set("THETA",args[3]);
        Job job=Job.getInstance(conf,"n-gram relative frequency count");
        job.setJarByClass(NgramRF.class);
        job.setMapperClass(NgramRF.NRFMapper.class);
        job.setReducerClass(NgramRF.NRFReducer.class);
//        job.setReducerClass(NgramRF.DummyReducer.class);
        job.setCombinerClass(NgramRF.NRFCombiner.class);
        job.setPartitionerClass(NgramRF.WordArrayPartitioner.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.setMapOutputKeyClass(WordArrayWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
