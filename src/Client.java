import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;


// Does a Put, Get and a Scan against an hbase table.

public class Client {

  public static void main(String[] args) throws IOException {
	ArrayList<String> list = new ArrayList<String>();
	Random rand = new Random();
	Path path1;
	String keyword;
    path1 = new Path("/user/root/output/results_4/part2/part-r-00000");
    FileSystem fileSystem = FileSystem.get(new Configuration());
    BufferedReader bufferedReader1 = new BufferedReader(new InputStreamReader(fileSystem.open(path1)));
    
    
    for(int i=0; i<1000; i++){
    	keyword=bufferedReader1.readLine().split("\t")[0];
    	list.add(keyword);   	
    }
    
    Configuration config = HBaseConfiguration.create();
    HTable table = new HTable(config, "index");
    int counter=0;
for (int j =0; j<10000; j++){    
    String to_check = list.get(rand.nextInt(1000));
    Get g = new Get(Bytes.toBytes(to_check));
    Result r = table.get(g);
    if (!r.isEmpty())
    	counter++;
    	}
	int total = counter/100;
	System.out.println("Successful queries = " + total + "%");
    table.close();

  }
}