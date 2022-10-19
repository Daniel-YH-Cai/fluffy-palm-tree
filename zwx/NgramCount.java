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

public class NgramCount {
    public static int N;

    public static class NgramCountTextInputFormat extends TextInputFormat{
        @Override
        protected boolean isSplitable(JobContext context, Path file) {
            return false;
        }
    }

    public static class NgramCountMapper extends Mapper<Object, Text, Text, MapWritable>{

        private final static IntWritable one = new IntWritable(1);
        private MapWritable H = new MapWritable();
        private Text word = new Text();
        LinkedList<String> inputText = new LinkedList<String>(); // save last n - 1 words

        protected void setup(Context context) throws IOException, InterruptedException {
            N = Integer.parseInt(context.getConfiguration().get("N"));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] mapInput = value.toString().split("\\W+");
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

            for (int i = 0 ; i <= inputText.size() - N ; i++ ) { //0 1 2 3 4 5 length = 6  N = 2
                if ( !hashSet.contains(inputText.get(i)) ){
                    hashSet.add(inputText.get(i));
                    H.clear();
                    word.set (inputText.get(i));
                    for (int j = i ; j <= inputText.size() -N ; j++ ){
                        if (inputText.get(j).equals(inputText.get(i)) ){
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
                    context.write(word, H);
                }
                inputText.remove(i); // remains N - 1 words for next line
                i--;
            }
            hashSet.clear();
        }
    }



    public static class NgramCountReducer extends Reducer<Text,MapWritable,Text,IntWritable> {

        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

            MapWritable sumMap = new MapWritable();

            for (MapWritable val : values) {
                Set<Writable> keys = val.keySet();
                for (Writable k : keys){
                    if(sumMap.containsKey(k))
                        ((IntWritable)sumMap.get(k)).set( ((IntWritable)sumMap.get(k)).get() + ((IntWritable)val.get(k)).get() );
                    else
                        sumMap.put( k , val.get(k));
                }
            }
            Set<Writable> sumKeys = sumMap.keySet();
            for (Writable k : sumKeys){
                Text sequence = new Text();
                sequence.set(key.toString() + " " + k.toString());
                IntWritable result = (IntWritable) sumMap.get(k);
                context.write(sequence, result);

            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("N", args[2]);

        Job job = Job.getInstance(conf, "N-grams count");
        job.setJarByClass(NgramCount.class);
        job.setMapperClass(NgramCountMapper.class);
        //job.setCombinerClass(NgramCountReducer.class);
        job.setReducerClass(NgramCountReducer.class);
        job.setInputFormatClass(NgramCount.NgramCountTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
