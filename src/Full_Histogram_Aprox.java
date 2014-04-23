import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class Full_Histogram_Aprox {
 public static Set<String> stop_words = new HashSet<String>();
 
 
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable one = new IntWritable(1);
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
        	//String[] keywords = value.toString().split("\t")[1].split(" ");
        	String[] keywords = value.toString().split("_");
        	char upp;
        	for (String s : keywords){
        		if (characters.contains(s.charAt(0)))
        			word.set("symbol");
        		else if (s.charAt(0)>='0'&& s.charAt(0)<='9' )
        			word.set("number");
        		else if ((upp = Character.toUpperCase(s.charAt(0))) >='A'
        			  && (upp = Character.toUpperCase(s.charAt(0))) <='Z')
        			if (!stop_words.contains(s))
        				word.set(Character.toString(upp));
        			else 
        				continue;
        		context.write(word, one);
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
     
     
     public static class Map2 extends Mapper<LongWritable, Text, Text, IntWritable> {
    	 private static IntWritable number = new IntWritable();
    	 private final static int iterations = 1000;
    	 private Text counter = new Text();
    	 	public void map(LongWritable key, Text value, Context context) 
    	 			throws IOException, InterruptedException {
    	 			String line = value.toString().split("\t")[1];
    	 		    int x = Integer.parseInt(line);
    	 			counter.set("total");
    	 			number.set(x);
    	 			context.write(counter, number);
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
     
     
     public static class Reduce2 extends Reducer<Text, IntWritable, Text, IntWritable>{
    	 public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
   		 		int sum = 0;
   		 		for(IntWritable val : values){
   		 			sum+= val.get();
   		 		}
   		 		context.write(key, new IntWritable(sum));
   	 }
    	 
     }
     
     public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();        
        Job job = new Job(conf, "full_histogram_aprox");

        Path hdfsPath = new Path("/user/root/input/english.stop");
        DistributedCache.addCacheFile(hdfsPath.toUri(), job.getConfiguration());
        
        
        job.setJarByClass(Full_Histogram_Aprox.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));        
        job.waitForCompletion(true);

     
        Job job2 = new Job(conf, "full_histogram_aprox");

        job2.setJarByClass(Full_Histogram_Aprox.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);        
        job2.setMapperClass(Map2.class);
        job2.setCombinerClass(Reduce2.class);
        job2.setReducerClass(Reduce2.class);        
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);        
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));        
        job2.waitForCompletion(true);
        
        
        Path path1 = new Path("/user/root/output/results_5_2_2/part1/part-r-00000");
        Path path2 = new Path("/user/root/output/results_5_2_2/part2/part-r-00000");
        FileSystem fileSystem = FileSystem.get(new Configuration());
        BufferedReader bufferedReader1 = new BufferedReader(new InputStreamReader(fileSystem.open(path1)));
        BufferedReader bufferedReader2 = new BufferedReader(new InputStreamReader(fileSystem.open(path2)));
        double total = Integer.parseInt(bufferedReader2.readLine().split("\t")[1]);
        bufferedReader2.close();
        BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fileSystem.create(path2, true)));
        String line;
        while ((line = bufferedReader1.readLine()) != null){
        	String lin[] = line.split("\t");
        	double percen = (Integer.parseInt(lin[1])/total)*100;
        	lin[1] = Double.toString(percen).substring(0, 4);
        	br.write(lin[0]+"\t"+lin[1]+"\n");
       }
        br.close();
        
     }        
   }