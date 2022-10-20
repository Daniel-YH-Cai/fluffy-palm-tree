import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Arrays;
import java.util.LinkedList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NgramRF {
    public static int N;

    public static double theta;
    public static class NgramRFTextInputFormat extends TextInputFormat{
        @Override
        protected boolean isSplitable(JobContext context, Path file) {
            return false;
        }
    }

    public static class NgramRFMapper extends Mapper<Object, Text, Text, MapWritable>{

        private final static IntWritable one = new IntWritable(1);
        private MapWritable H = new MapWritable();
        private Text word = new Text();
        LinkedList<String> inputText = new LinkedList<String>(); // save last n - 1 words

        protected void setup(Context context) throws IOException, InterruptedException {
            N = Integer.parseInt(context.getConfiguration().get("N"));
            theta = Double.parseDouble(context.getConfiguration().get("theta"));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] mapInput = value.toString().split("\\W+");

            if (N == 1){
                for (int i = 0 ; i < mapInput.length ; i++){
                    if (!mapInput[i].equals("")){
                        word.set (mapInput[i]);
                        context.write(word, H);
                    }

                }
                return ;
            }
            List<String> temp = Arrays.asList(mapInput);
            inputText.addAll(temp);
            for (int i = 0 ; i < inputText.size() ; i++ ){ // remove ""
                if (inputText.get(i).equals("")){
                    inputText.remove(i);
                    i--;
                }
            }
            if(inputText.size() < N ){ // no word in current line or to short to form Ngram
                return;
            }

            HashSet<String> hashSet = new HashSet<String>();
            Text subSumKey = new Text("*");
            IntWritable subSum = new IntWritable();
            for (int i = 0 ; i <= inputText.size() - N ; i++ ) { //0 1 2 3 4 5 length = 6  N = 2
                if ( !hashSet.contains(inputText.get(i)) ){
                    hashSet.add(inputText.get(i));
                    H.clear();
                    word.set (inputText.get(i));
                    int count = 0;
                    for (int j = i ; j <= inputText.size() -N ; j++ ){
                        if (inputText.get(j).equals(inputText.get(i)) ){
                            count = count + 1;
                            Text neighbor = new Text();
                            String u = "";
                            for (int k = j + 1 ; k < j + N ; k++){
                                u = u + " " + inputText.get(k);
                            }
                            neighbor.set(u);
                            if(H.containsKey(u)) {
                                IntWritable t = (IntWritable) H.get(neighbor);
                                t.set(t.get() + 1);
                                H.put(neighbor, t);
                            }
                            else
                                H.put( neighbor , one );
                        }
                    }
                    subSum.set(count);
                    H.put( subSumKey , subSum );
                    context.write(word, H);
                }
                inputText.remove(i); // remains N - 1 words for next line
                i--;
            }
            hashSet.clear();
        }
    }



    public static class NgramRFCombiner extends Reducer<Text,MapWritable,Text,MapWritable> {

        protected void setup(Reducer.Context context) throws IOException, InterruptedException {
            N = Integer.parseInt(context.getConfiguration().get("N"));
            theta = Double.parseDouble(context.getConfiguration().get("theta"));
        }

        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

            MapWritable sumMap = new MapWritable();
            Text subSumKey = new Text("*");

            if (N == 1){
                context.write(key, sumMap);
                return ;
            }

            int sum = 0;
            for (MapWritable val : values) {
                Set<Writable> keys = val.keySet();
                sum = sum + ((IntWritable)val.get(subSumKey)).get();

                for (Writable k : keys){
                    if(sumMap.containsKey(k))
                        ((IntWritable)sumMap.get(k)).set( ((IntWritable)sumMap.get(k)).get() + ((IntWritable)val.get(k)).get() );
                    else
                        sumMap.put( k , val.get(k));
                }
                context.write(key, sumMap);
            }

        }
    }

    public static class NgramRFReducer extends Reducer<Text,MapWritable,Text,DoubleWritable> {

        protected void setup(Reducer.Context context) throws IOException, InterruptedException {
            N = Integer.parseInt(context.getConfiguration().get("N"));
            theta = Double.parseDouble(context.getConfiguration().get("theta"));
        }

        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

            MapWritable sumMap = new MapWritable();
            Text subSumKey = new Text("*");

            if (N == 1){
                DoubleWritable one = new DoubleWritable(1.0);
                context.write(key, one);
                return ;
            }

            DoubleWritable result = new DoubleWritable();
            int sum = 0;
            for (MapWritable val : values) {
                Set<Writable> keys = val.keySet();
                sum = sum + ((IntWritable)val.get(subSumKey)).get();

                for (Writable k : keys){
                    if(sumMap.containsKey(k))
                        ((IntWritable)sumMap.get(k)).set( ((IntWritable)sumMap.get(k)).get() + ((IntWritable)val.get(k)).get() );
                    else
                        sumMap.put( k , val.get(k));
                }
            }
            Set<Writable> sumKeys = sumMap.keySet();
            for (Writable k : sumKeys){
                if ( !((Text)k).equals(subSumKey) ){
                    double freq = ((IntWritable) sumMap.get(k)).get() / (double) sum;
                    if (freq >= theta) {
                        Text sequence = new Text();
                        sequence.set(key.toString() + k.toString());
                        result.set(freq);
                        context.write(sequence, result);
                    }
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("N", args[2]);
        conf.set("theta", args[3]);
        Job job = Job.getInstance(conf, "N-grams RF");
        job.setJarByClass(NgramRF.class);
        job.setMapperClass(NgramRFMapper.class);
        job.setCombinerClass(NgramRFCombiner.class);
        job.setReducerClass(NgramRFReducer.class);
        job.setInputFormatClass(NgramRF.NgramRFTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
