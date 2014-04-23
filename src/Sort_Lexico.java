import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;



import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;



public class Sort_Lexico {
 public static Set<String> stop_words = new HashSet<String>();
 
 
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
      private final Text ref = new Text("ref");
      private final static int iterations = 1000;
      private static Set<Character> characters = new HashSet<Character>();
      private Text word = new Text(); 
      
      @Override
 	  protected void setup(Context context) throws IOException, InterruptedException{
    	 Configuration conf = context.getConfiguration();
 		 characters.add('~'); characters.add('!'); characters.add('@'); characters.add('#');
 		 characters.add('$'); characters.add('%'); characters.add('^'); characters.add('&');
 		 characters.add('*'); characters.add('('); characters.add(')'); characters.add('_');
 		 characters.add('+'); characters.add('{'); characters.add('}'); characters.add('|');
 		 characters.add(':'); characters.add('”'); characters.add('<'); characters.add('>');
 		 characters.add('?'); characters.add('['); characters.add(']'); characters.add('\\');
 		 characters.add(';'); characters.add('’'); characters.add(','); characters.add('.');
 		 characters.add('/');
 		 
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
        	String[] keywords = value.toString().split("_");
        	
        	for (String s : keywords){
        		if (!stop_words.contains(s)){
                    word.set(s.toLowerCase());
        			context.write(ref, word);
        		}        		
        	}
        }
        @Override
        public void run(Context context) throws IOException, InterruptedException{
        	setup(context);
        	int count = 0 ;
            while (context.nextKeyValue()) {
                if(count++ < iterations){ // check if enough records has been processed already
                    map(context.getCurrentKey(), context.getCurrentValue(), context);
                }else{
                    break;
                }
        }
       }        
     }         

 	public static class Reduce extends Reducer<Text, Text, Text, NullWritable> { 
    	 public void reduce(Text key, Iterable<Text> values, Context context)
          throws IOException, InterruptedException {
    		List<String> list = new ArrayList<String>();
            for (Text val : values) {
                list.add(val.toString());
            }
            Collections.sort(list);
            int range= list.size()/10;
            for(int i=1; i<10;i++)
            	context.write(new Text(list.get(i*range).toString()), NullWritable.get());
        }
     }   
     
     
 	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
 	      private final Text ref = new Text("ref");
 	      private static Set<Character> characters = new HashSet<Character>();
 	      private Text word = new Text(); 
 	      
 	      @Override
 	 	  protected void setup(Context context) throws IOException, InterruptedException{
 	    	 Configuration conf = context.getConfiguration();
 	 		 characters.add('~'); characters.add('!'); characters.add('@'); characters.add('#');
 	 		 characters.add('$'); characters.add('%'); characters.add('^'); characters.add('&');
 	 		 characters.add('*'); characters.add('('); characters.add(')'); characters.add('_');
 	 		 characters.add('+'); characters.add('{'); characters.add('}'); characters.add('|');
 	 		 characters.add(':'); characters.add('”'); characters.add('<'); characters.add('>');
 	 		 characters.add('?'); characters.add('['); characters.add(']'); characters.add('\\');
 	 		 characters.add(';'); characters.add('’'); characters.add(','); characters.add('.');
 	 		 characters.add('/');
 	 		 
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
 	        	String[] keywords = value.toString().split("_");
 	        	
 	        	for (String s : keywords){
 	        		if (!stop_words.contains(s)){
 	                    word.set(s.toLowerCase());
 	        			context.write(word,ref);
 	        		}        		
 	        	}
 	        }     
 	
 	}
 	
 public static class Reduce2 extends Reducer<Text, Text, Text, NullWritable> { 
   	 public void reduce(Text key, Iterable<Text> values, Context context)
         throws IOException, InterruptedException {
   		 context.write(key, NullWritable.get());
       }
    }   
 	
 	
     public static void main(String[] args) throws Exception {
       /* Configuration conf = new Configuration();        
        Job job = new Job(conf, "sort_lexico");

        Path hdfsPath = new Path("/user/root/input/english.stop");
        DistributedCache.addCacheFile(hdfsPath.toUri(), job.getConfiguration());
        conf.addResource(new Path("/opt/hadoop-1.2.1/conf/core-site.xml")); 
        conf.addResource(new Path("/opt/hadoop-1.2.1/conf/hdfs-site.xml"));
        
        job.setJarByClass(Sort_Lexico.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);      
        FileInputFormat.addInputPath(job, new Path(args[0]));
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));        
        job.waitForCompletion(true);
        
        
        conf = new Configuration();
        Job job2 = new Job(conf, "sort_lexico");
        job2.setJarByClass(Sort_Lexico.class);
        TotalOrderPartitioner.setPartitionFile(conf, new Path(args[1]+"part-r-00000"));
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);        
        job2.setMapperClass(Map2.class);        
        job2.setNumReduceTasks(10);
        job2.setReducerClass(Reduce2.class);        
        job2.setPartitionerClass(TotalOrderPartitioner.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
                
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));        
        job2.waitForCompletion(true);
*/
    	 
    	 Configuration conf = new Configuration();
    	    conf.addResource(new Path("/opt/hadoop-1.2.1/conf/core-site.xml"));
    	    conf.addResource(new Path("/opt/hadoop-1.2.1/conf/hdfs-site.xml"));
    	 
    	    DistributedCache.addCacheFile(new URI("/user/root/input/english.stop"), conf);
    	     
    	    
    	     
    	    // create a new Job
    	    Job job = new Job(conf, "sort_lexico");
    	    job.setJarByClass(Sort_Lexico.class);
    	     
    	    // set the intermediate key and value types
    	    job.setMapOutputKeyClass(Text.class);
    	    job.setMapOutputValueClass(Text.class);
    	     
    	    job.setOutputKeyClass(Text.class);
    	    job.setOutputValueClass(NullWritable.class);
    	     
    	    // set the mapper and the reducer
    	    job.setMapperClass(Map.class);
    	    job.setReducerClass(Reduce.class);
    	     
    	    // one reducer only, this MR will create the partition file
    	    job.setNumReduceTasks(1);
    	    //sets the input format for the job and the output format
    	    job.setInputFormatClass(TextInputFormat.class);
    	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    	     
    	    FileInputFormat.addInputPath(job, new Path(args[0]));
    	    SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
    	     
    	    // Submit the job to the cluster and wait for it to finish.
    	    job.waitForCompletion(true);
    	     
    	    System.out.println("Partition file created...");
    	     
    	     
    	    conf = new Configuration();
    	    conf.addResource(new Path("/opt/hadoop-1.2.1/conf/core-site.xml"));
    	    conf.addResource(new Path("/opt/hadoop-1.2.1/conf/hdfs-site.xml"));
    	    TotalOrderPartitioner.setPartitionFile(conf , new Path(args[1]+"/part-r-00000"));
    	    DistributedCache.addCacheFile(new URI("/user/root/input/english.stop"), conf);
    	    
    	    job = new Job(conf, "sort_lexico");
    	    job.setJarByClass(Sort_Lexico.class);
    	     
    	    TotalOrderPartitioner.setPartitionFile(conf , new Path(args[1]+"part-r-00000"));
    	     
    	    // set the intermediate key and value types
    	    job.setMapOutputKeyClass(Text.class);
    	    job.setMapOutputValueClass(Text.class);
    	     
    	    job.setOutputKeyClass(Text.class);
    	    job.setOutputValueClass(NullWritable.class);
    	     
    	    // set the mapper and the reducer
    	    job.setMapperClass(Map2.class);
    	    job.setReducerClass(Reduce2.class);
    	    job.setNumReduceTasks(10);
    	    job.setInputFormatClass(TextInputFormat.class);
    	    job.setOutputFormatClass(TextOutputFormat.class);
    	    job.setPartitionerClass(TotalOrderPartitioner.class); 
    	     
    	    FileInputFormat.addInputPath(job, new Path(args[0]));
    	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    	         
    	    // Submit the job to the cluster and wait for it to finish.
    	    job.waitForCompletion(true);
    	     
        
     }        
   }