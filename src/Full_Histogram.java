import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class Full_Histogram {
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable one = new IntWritable(1);
      private static Set<Character> characters = new HashSet<Character>();
      private Text word = new Text(); 
      
      @Override
 	  protected void setup(Context context) throws IOException, InterruptedException{
 		 characters.add('~'); characters.add('!'); characters.add('@'); characters.add('#');
 		 characters.add('$'); characters.add('%'); characters.add('^'); characters.add('&');
 		 characters.add('*'); characters.add('('); characters.add(')'); characters.add('_');
 		 characters.add('+'); characters.add('{'); characters.add('}'); characters.add('|');
 		 characters.add(':'); characters.add('”'); characters.add('<'); characters.add('>');
 		 characters.add('?'); characters.add('['); characters.add(']'); characters.add('\\');
 		 characters.add(';'); characters.add('’'); characters.add(','); characters.add('.');
 		 characters.add('/');
 	 }
      
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {       	
        	String[] keywords = value.toString().split("\t")[1].split(" ");
        	char upp;
        	for (String s : keywords){
        		if (characters.contains(s.charAt(0)))
        			word.set("symbol");
        		else if (s.charAt(0)>='0'|| s.charAt(0)<='9' )
        			word.set("number");
        		else if ((upp = Character.toUpperCase(s.charAt(0))) >='A'
        			  || (upp = Character.toUpperCase(s.charAt(0))) <='Z')
        			word.set(Character.toString(upp));
        			context.write(word, one);
        	}
        	
        }
     }         
     public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> { 
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
          throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
     }        
     public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();        
        Job job = new Job(conf, "full_histogram");

        job.setJarByClass(Full_Histogram.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));        
        job.waitForCompletion(true);
     }        
   }