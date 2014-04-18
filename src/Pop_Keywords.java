import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*;

public class Pop_Keywords extends Configured {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	

	}         

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> { 
        

	}        
    
	public static void main(String[] args) throws Exception {
		/*JobConf conf = new JobConf(getConf(), WordCount.class);
	     conf.setJobName("wordcount");
	
	     conf.setOutputKeyClass(Text.class);
	     conf.setOutputValueClass(IntWritable.class);
	     
	     conf.setMapperClass(Map.class);
	     conf.setCombinerClass(Reduce.class);
	     conf.setReducerClass(Reduce.class);
	
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	
	     List<String> other_args = new ArrayList<String>();
	     for (int i=0; i < args.length; ++i) {
	       if ("-skip".equals(args[i])) {
	         DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
	         conf.setBoolean("wordcount.skip.patterns", true);
	       } else {
	         other_args.add(args[i]);
	       }
	     }
	
	     FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
	     FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
	
	     JobClient.runJob(conf);    
        	*/
        }
        
    	 
     }        
   