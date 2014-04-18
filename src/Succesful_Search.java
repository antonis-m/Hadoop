import java.io.IOException;
//import java.util.*;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class Succesful_Search {
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            if(line.length > 3){
            	word.set("succesful");
            	context.write(word, one);
            } else {
            	word.set("unsuccesful");
            	context.write(word,one);
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
 	
 public static class Map2 extends Mapper<LongWritable, Text, Text, IntWritable>{
	 private static IntWritable number = new IntWritable();
	 private Text counter = new Text();
	 	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	 			String line = value.toString().split("\t")[1];
	 		    int x = Integer.parseInt(line);
	 			counter.set("total");
	 			number.set(x);
	 			context.write(counter, number);
	 	}
 }
 
 public static class Reduce2 extends Reducer<Text, IntWritable, Text, IntWritable> {
	 public void reduce(Text key, Iterable<IntWritable> values, Context context)
	          throws IOException, InterruptedException {
		 		int sum = 0;
		 		for(IntWritable val : values){
		 			sum+= val.get();
		 		}
		 		context.write(key, new IntWritable(sum));
	 }
 }
 
     public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();        
        Job job = new Job(conf, "success_search");

        job.setJarByClass(Succesful_Search.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));        
        job.waitForCompletion(true);
        
        Job job2 = new Job(conf, "success_search");

        job2.setJarByClass(Succesful_Search.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);        
        job2.setMapperClass(Map2.class);
        job2.setReducerClass(Reduce2.class);        
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);        
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));        
        job2.waitForCompletion(true);
        
        Path path1 = new Path("/user/root/output/results_2/part1/part-r-00000");
        Path path2 = new Path("/user/root/output/results_2/part2/part-r-00000");
        FileSystem fileSystem = FileSystem.get(new Configuration());
        BufferedReader bufferedReader1 = new BufferedReader(new InputStreamReader(fileSystem.open(path1)));
        BufferedReader bufferedReader2 = new BufferedReader(new InputStreamReader(fileSystem.open(path2)));
        double successful = Integer.parseInt(bufferedReader1.readLine().split("\t")[1]);
        double unsuccessful = Integer.parseInt(bufferedReader1.readLine().split("\t")[1]);
        double total = Integer.parseInt(bufferedReader2.readLine().split("\t")[1]);
        
        successful = (successful/total)*100;
        unsuccessful = (unsuccessful/total)*100;
        
        BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fileSystem.create(path2, true)));  
        String line = "successful as percentage " + successful + "%.";
        br.write(line);
        br.write("\n");
        String line2= "unsuccessful as percentage " + unsuccessful +"%.";
        br.write(line2);
        br.write("\n");
        br.close();
        
     }        
   }