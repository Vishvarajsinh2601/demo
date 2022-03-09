package lab01_q6.Lab01;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class https_responsemapper extends Mapper<LongWritable,Text,Text,Text> {

	public void map(LongWritable key, Text val, Context c) 
			throws IOException, InterruptedException {
		String ts = val.toString().split(" ")[3];
		ts=ts.substring(1);
		String status= val.toString().split(" ")[8];
		String url=val.toString().split(" ")[6];
		if(status.equals("404") && !url.equals("-"))
		{
			String s=ts + " " +url;
			c.write(new Text(status), new Text( s ) );
		}
	}
}
