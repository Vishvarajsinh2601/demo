package lab01_q2.Lab01;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;

public class MaxSalaryReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int maxvalue= Integer.MIN_VALUE;
		for (IntWritable value : values) 
			{
			maxvalue= Math.max(maxvalue,value.get());
			}
		
		context.write(key, new DoubleWritable(maxvalue));
	}
}