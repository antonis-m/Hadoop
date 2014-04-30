import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import org.apache.hadoop.fs.FileSystem;
//import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Wiki_In_Results {
	
 public static class Map_Wiki extends Mapper<LongWritable, Text, Text, Text> {
      private final static Text zero = new Text("0");
      private Text word = new Text();        
        public void map(LongWritable key, Text value, Context context) 
        		throws IOException, InterruptedException {
        	String line[] = value.toString().split("_");
        	for (String s : line){
        		word.set(s);
        		context.write(word, zero);
        	}
        }
     }
 
 public static class Map_Aol extends Mapper<LongWritable, Text, Text, Text>{
	 private static Text query_key = new Text();
	 private static Text word = new Text();
	 public void map(LongWritable key, Text value, Context context) 
			 throws IOException, InterruptedException{
		 String line[] = value.toString().split("\t");
		 if(! line[0].equals("AnonID")){
		 String timedate[] = line[2].split(" ");
		 String keywords[] = line[1].split(" ");
		 String timestamp = timedate[0]+"_"+timedate[1];
		 query_key.set(line[0]+"_"+timestamp);
		 for (String s : keywords){
			 word.set(s);
			 context.write(word, query_key);
		 }
		}
	 }
 }
 
     public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
    	 private static final Text zero = new Text("0");
    	 private static final Text exists = new Text("1");
    	 private static Text keyword = new Text();
    	 public void reduce(Text key, Iterable<Text> values, Context context)
          throws IOException, InterruptedException {
    		 ArrayList<String> list = new ArrayList<String>();
    		 for (Text val: values)
    			 list.add(val.toString());
    		 
    		 if(list.contains("0")){
    			 for (String s : list)
    				 if (!s.equals("0")){
    					 keyword.set(s);
    					 context.write(exists,keyword);
    				 }
    		 } else 
    			 for (String s : list){
    				 keyword.set(s);
    				 context.write(zero, keyword);
    			 }
    	 }
     } 
     
     public static class Map2 extends Mapper<LongWritable, Text, Text, Text>{
    	 private static Text query_key = new Text();
    	 private static Text exists = new Text();
    	 public void map(LongWritable key, Text value, Context context) 
    			 throws IOException, InterruptedException{
    		 	 String line[] = value.toString().split("\t");
    		 	 query_key.set(line[1]);
    		 	 exists.set(line[0]);
    			 context.write(exists,query_key);
    		 
    	 }
     }

     public static class Reduce2 extends Reducer<Text, Text, Text, IntWritable> { 
    	 private static Text Q = new Text();
    	 private static IntWritable total = new IntWritable();
    	 public void reduce(Text key, Iterable<Text> values, Context context)
          throws IOException, InterruptedException {
        	int sum=0;
        
        	for(@SuppressWarnings("unused") Text s : values)
        		sum+=1;
        	
        	if(key.toString().equals("1")){
        		Q.set("successful");
        		total.set(sum);
        		context.write(Q, total);
        	} else if(key.toString().equals("0")){
        		Q.set("unsuccessful");
        		total.set(sum);
        		context.write(Q, total);
        	}
        }
     } 

     
     
     public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();        
        Job job = new Job(conf, "wiki_in_results");

        job.setJarByClass(Wiki_In_Results.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);        
        job.setReducerClass(Reduce1.class);
        
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Map_Wiki.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Map_Aol.class);
        job.setOutputFormatClass(TextOutputFormat.class);                
        FileOutputFormat.setOutputPath(job, new Path(args[2]));        
        job.waitForCompletion(true);
        
        Job job2 = new Job(conf, "wiki_in_results");

        job2.setJarByClass(Wiki_In_Results.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);        
        job2.setMapperClass(Map2.class);
        job2.setNumReduceTasks(2);
        job2.setReducerClass(Reduce2.class);        
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);        
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));        
        job2.waitForCompletion(true);
        
        
        Path path1 = new Path("/user/root/output/results_9/part2/part-r-00000");
        Path path2 = new Path("/user/root/output/results_9/part2/part-r-00001");
        FileSystem fileSystem = FileSystem.get(new Configuration());
        BufferedReader bufferedReader1 = new BufferedReader(new InputStreamReader(fileSystem.open(path1)));
        BufferedReader bufferedReader2 = new BufferedReader(new InputStreamReader(fileSystem.open(path2)));
        double successful = Integer.parseInt(bufferedReader1.readLine().split("\t")[1]);
        double unsuccessful = Integer.parseInt(bufferedReader2.readLine().split("\t")[1]);
        
        bufferedReader1.close();
        bufferedReader2.close();

        String successful_per = Double.toString(((successful/(successful+unsuccessful))*100)).substring(0, 4);
        String unsuccessful_per = Double.toString((unsuccessful/(successful+unsuccessful)*100)).substring(0, 4);
        
        BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fileSystem.create(path1, true)));
        br.write("Successful Queries as percentage "+successful_per + " "+"%\n");
        br.write("Unsuccessful Queries as percentage "+unsuccessful_per + " "+"%\n");
        br.close();
     }        
   }