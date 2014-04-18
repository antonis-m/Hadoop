import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class Pages_Users {
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable UserId = new IntWritable();
      private Text word = new Text();        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line[] = value.toString().split("\t");
            if (line.length > 3 ){
            	if(! line[0].equals("AnonID")){
            	UserId.set(Integer.parseInt(line[0]));
            	word.set(line[4]);
            	context.write(word, UserId);
            	}            	
            }
        }
     }         

 
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> { 
     private static Hashtable<String, String> table = new Hashtable<String, String>();
     private IntWritable Users = new IntWritable();
	 public void reduce(Text key, Iterable<IntWritable> values, Context context)
          throws IOException, InterruptedException {
		 	int sum=0;
		 	for (IntWritable val : values) {
                if (table.containsKey(val))
                	continue;
                else {
                	table.put(val.toString(), key.toString());
                	sum++;
                }                
            }
		 	if (sum >= 10){
            	Users.set(sum);
            	context.write(key,Users);
            }
        }
     }        
     public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();        
        Job job = new Job(conf, "pages_users");

        job.setJarByClass(Pages_Users.class);
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