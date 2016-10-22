package tvUnitSalesCalculation;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceTask extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	private IntWritable outputVal;
	
	@Override
	public void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		outputVal = new IntWritable();		
		super.setup(context);
	}
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> inputVals,
			Context context) 
		throws IOException, InterruptedException {
		int totalUnit = 0;
		//count how many 
		while(inputVals.iterator().hasNext()){
			totalUnit++;
			inputVals.iterator().next();
		}
		outputVal.set(totalUnit);
		
		context.write(key, outputVal);
	}
}
