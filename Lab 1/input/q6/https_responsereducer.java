package lab01_q6.Lab01;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class https_responsereducer extends Reducer<Text,Text,Text,Text> {
	public void reduce(Text key,Text values, Context c) 
			throws IOException, InterruptedException {
		
		c.write(key, new Text(values));
	}
}

