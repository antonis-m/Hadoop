import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
//import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/*import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;*/
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Bulk_Loading{
	
	public static class HBaseKVMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
		private ImmutableBytesWritable hKey = new ImmutableBytesWritable();
		private KeyValue kv;
		private static final byte[] SRV_COL_FAM = "title".getBytes();
		private static final byte[] name = "name".getBytes();
		String tableName = "content";
		
		public void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException {
			
			String title = value.toString();
			MessageDigest m;
			try {
				m = MessageDigest.getInstance("MD5");
				m.reset();
				m.update(title.getBytes());
				byte[] digest = m.digest();
				
				BigInteger bigInt = new BigInteger(1,digest);
				String hashtext = bigInt.toString(8);
				hKey.set(hashtext.getBytes());
				kv= new KeyValue(hKey.get(),SRV_COL_FAM,name,title.getBytes());
				context.write(hKey, kv);
				
			
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}			
		}	

}
public static void main(String[] args) throws Exception {
	
    Configuration conf = new Configuration();
  
    HBaseConfiguration.addHbaseResources(conf);
    Job job = new Job(conf, "bulk_loading");
    job.setJarByClass(HBaseKVMapper.class);

    job.setMapperClass(HBaseKVMapper.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(KeyValue.class);

    job.setInputFormatClass(TextInputFormat.class);
    
    
    HTable hTable = new HTable(conf, args[2]);
    
    // Auto configure partitioner and reducer
    HFileOutputFormat.configureIncrementalLoad(job, hTable);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }
}