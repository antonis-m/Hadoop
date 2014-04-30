import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Inverted_Index {
 public static class HMap extends TableMapper<ImmutableBytesWritable,KeyValue>{
	 private ImmutableBytesWritable hKey = new ImmutableBytesWritable();
	 private KeyValue kv;
	 private static final byte[] CF = "title".getBytes();
	 private static final byte[] CF2 = "hash_keys".getBytes();
	 private static final byte[] qualifier = "name".getBytes() ;
	 private static byte[] qualifier2 ;
	 public void map(ImmutableBytesWritable row, Result value, Context context) 
			 throws IOException, InterruptedException{
		 
		 String rkey = Bytes.toString(row.get()); //hashkey
		 String valu  = Bytes.toString(value.getValue(CF, qualifier));
		 String val[] = valu.split("_");		 

		 
		 
		 for (String s : val){
			 hKey.set(s.getBytes());
			 qualifier2 = rkey.getBytes(); 
			 kv= new KeyValue(hKey.get(),CF2,qualifier2,valu.getBytes());
			 context.write(hKey,kv);
		 }
	 }
 }
  
     public static void main(String[] args) throws Exception {
        
    	 Configuration conf = HBaseConfiguration.create();
    	 Job job = new Job(conf, "inverted_index");
    	 job.setJarByClass(Inverted_Index.class);     // class that contains mapper

    	 
    	 Scan scan = new Scan();
    	 scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
    	 scan.setCacheBlocks(false);  // don't set to true for MR jobs
    	 
    	 
    	 

    	 TableMapReduceUtil.initTableMapperJob(
    	   args[0].getBytes(),        		// input HBase table name
    	   scan,            		  		// Scan instance to control CF and attribute selection
    	   HMap.class,   			  		// mapper
    	   ImmutableBytesWritable.class,    // mapper output key
    	   KeyValue.class,        	        // mapper output value
    	   job);

    	 
    
    	 	HTable hTable = new HTable(conf, args[2]);
    	    
    	    // Auto configure partitioner and reducer
    	    HFileOutputFormat.configureIncrementalLoad(job, hTable);
    	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	    job.waitForCompletion(true);
     }        
   }