import java.util.List;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

 
import com.sun.org.apache.xalan.internal.xsltc.runtime.Hashtable;
 
 
public class ordered_sample {
 
    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
           
          private final static IntWritable one = new IntWritable(1);
          private Text word = new Text();
          private Hashtable stop_words = new Hashtable();
           
          @Override
          public void setup(Context context) throws IOException, InterruptedException{
              Configuration conf = context.getConfiguration();
              Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
 
              BufferedReader readBuffer = new BufferedReader(new FileReader(localFiles[0].toString()));
              String line;
              while ((line=readBuffer.readLine())!=null)
                  stop_words.put(line, "1");
               
              readBuffer.close();
               
            }
           
          @Override
          public void run(Context context) throws IOException, InterruptedException {
              int m;
              int iter = 0;
              if (context.getConfiguration().get("number") != null) {
                  m = Integer.parseInt(context.getConfiguration().get("number"));
                  try {
                      while ((context.nextKeyValue())&&(iter < m)) {
                          map(context.getCurrentKey(), context.getCurrentValue(), context);
                          iter++;
                      }
                  }
                  finally {
                      cleanup(context);
                  }
              }
              else {
                  try {
                      while (context.nextKeyValue())
                          map(context.getCurrentKey(), context.getCurrentValue(), context);    
                  }
                  finally {
                      cleanup(context);
                  }  
              }
          }
          
          public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
              String line = value.toString();
              String[] words = line.split("_");
             
              
              for (String s:words)
                  if (!stop_words.containsKey(s)) {
                      word.set(s);
                      context.write(one,word);
                   
                  }
               
        }
 
}
 
public static class Reduce extends Reducer<IntWritable, Text, Text, NullWritable> {
     
   
   public void reduce(IntWritable key, Iterable<Text> values, Context context)
     throws IOException, InterruptedException {
       List<String> samples = new ArrayList<String>();
        
       Text word = new Text();
       for (Text s : values)
           samples.add(s.toString());
       
       Collections.sort(samples);
        
        
        
       int interval_size =  samples.size() / 10;
        
       System.out.println("Length = " + samples.size() + " Interval = " + interval_size);
       for (int i = 1; i < 10 ;i++){
           word.set(samples.get(i*interval_size).toString());
           context.write(word, NullWritable.get());
       }
            
   }
}
 
 
    public static class Map_sort extends Mapper<LongWritable, Text, Text, IntWritable> {
           
          private final static IntWritable one = new IntWritable(1);
          private Text word = new Text();
          private Hashtable stop_words = new Hashtable();
           
          @Override
          public void setup(Context context) throws IOException, InterruptedException{
              Configuration conf = context.getConfiguration();
              Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
 
              BufferedReader readBuffer = new BufferedReader(new FileReader(localFiles[0].toString()));
              String line;
              while ((line=readBuffer.readLine())!=null)
                  stop_words.put(line, "1");
               
              readBuffer.close();
               
            }
           
          @Override
          public void run(Context context) throws IOException, InterruptedException {
              int m;
              int iter = 0;
              if (context.getConfiguration().get("number") != null) {
                  m = Integer.parseInt(context.getConfiguration().get("number"));
                  try {
                      while ((context.nextKeyValue())&&(iter < m)) {
                          map(context.getCurrentKey(), context.getCurrentValue(), context);
                          iter++;
                      }
                  }
                  finally {
                      cleanup(context);
                  }
              }
              else {
                  try {
                      while (context.nextKeyValue())
                          map(context.getCurrentKey(), context.getCurrentValue(), context);    
                  }
                  finally {
                      cleanup(context);
                  }  
              }
          }
          
          public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
              String line = value.toString();
              String[] words = line.split("_");
             
              
              for (String s:words)
                  if (!stop_words.containsKey(s)) {
                      word.set(s);
                      context.write(word, one);
                   
                  }
               
        }
 
}
 
public static class Reduce_sort extends Reducer<Text, IntWritable, Text, NullWritable> {
     
   
   public void reduce(Text key, Iterable<IntWritable> values, Context context)
     throws IOException, InterruptedException {
    
      // for (IntWritable i : values)
          context.write(key, NullWritable.get());
        
            
   }
}
/* args[0]: input for the M-R job
 * args[1]: output of the M-R job
 * args[2]: name of the file with the histogram created
 * args[3]: total execution number of each mapper
 */
 
/* If user gives args[3] then we make histogram_x
 * else we make the full histogram
 * **see run() in maper for more details
 */
public static void main(String[] args) throws Exception {
     
    Configuration conf = new Configuration();
    conf.addResource(new Path("/opt/hadoop-1.2.1/conf/core-site.xml"));
    conf.addResource(new Path("/opt/hadoop-1.2.1/conf/hdfs-site.xml"));
 
    DistributedCache.addCacheFile(new URI("/user/root/input/english.stop"), conf);
     
    if (args.length > 3)
        conf.setInt("number", Integer.parseInt(args[3]));
     
    // create a new Job
    Job job = new Job(conf, "make_partition_file");
    job.setJarByClass(ordered_sample.class);
     
    // set the intermediate key and value types
    job.setMapOutputKeyClass(IntWritable.class);
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
    
    job = new Job(conf, "sort_words");
    job.setJarByClass(ordered_sample.class);
     
    TotalOrderPartitioner.setPartitionFile(conf , new Path(args[1]+"part-r-00000"));
     
    // set the intermediate key and value types
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
     
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
     
    // set the mapper and the reducer
    job.setMapperClass(Map_sort.class);
    job.setReducerClass(Reduce_sort.class);
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