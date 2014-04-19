import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Popular_Keywords {
	public static Set<String> stop_words = new HashSet<String>();

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
 	 private final static IntWritable one = new IntWritable(1);
	 private Text word = new Text();

	 @Override
	 protected void setup(Context context) throws IOException, InterruptedException{
		 Configuration conf = context.getConfiguration();
		 try {
	     String stopwordCacheName = new Path("/user/root/input/english.stop").getName();
		 Path cacheFiles = DistributedCache.getLocalCacheFiles(conf)[0];		 
		 if (null != cacheFiles) {
		          if (cacheFiles.getName().equals(stopwordCacheName)) {		        	 
		            loadStopWords(cacheFiles);
		          }  		        
		      } 
	 } catch (IOException ioe) {
	      System.out.println("IOException reading from distributed cache");
	      System.out.println(ioe.toString());
	    }
	 }
	
	  void loadStopWords(Path cachePath) throws IOException {
		    // note use of regular java.io methods here - this is a local file now
		    BufferedReader wordReader = new BufferedReader(new FileReader(cachePath.toString()));
		    try {		    	
		    	String line;
		    	while((line=wordReader.readLine()) != null ){
		    		stop_words.add(line);
		    	} 
		    }finally {
		    		wordReader.close();
		    	}		    
		  }
		  
	 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
		 	
		 	String[] keyword = value.toString().split("\t")[1].split(" ");
		 	for(String s : keyword){
		 		if(!stop_words.contains(s)){
	            	word.set(s);
	            	context.write(word,one);
	            }	
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
	
	
	public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text>{
		private Text word = new Text();
		private IntWritable frequency = new IntWritable();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		    String line[] = value.toString().split("\t");
		    String new_key = line[0];
		    word.set(new_key);
		    frequency.set(Integer.parseInt(line[1].toString().trim()));
			context.write(frequency, word);			
		}
	}
	
	public static class Reduce2 extends Reducer<IntWritable, Text, Text, IntWritable>{
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
		          throws IOException, InterruptedException{			
			for (Text k : values){	
				context.write(k,key);
			}
		}
	}

	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();                
        Job job = new Job(conf, "popular_keywords");

        

        job.setJarByClass(Popular_Keywords.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class); 
        
        Path hdfsPath = new Path("/user/root/input/english.stop");
        DistributedCache.addCacheFile(hdfsPath.toUri(), job.getConfiguration());
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true); 
    	
        Job job2 = new Job(conf, "popular_keywords");

        job2.setJarByClass(Popular_Keywords.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);        
        job2.setMapperClass(Map2.class);
        //job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        job2.setReducerClass(Reduce2.class);        
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class); 
        
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        
        job2.waitForCompletion(true); 
                
    	 
     }        
   }