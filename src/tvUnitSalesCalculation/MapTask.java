package tvUnitSalesCalculation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapTask extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private Text keyOutput;
	private IntWritable outputVal;
	private String taskType;
	
	
	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		keyOutput = new Text();
		outputVal = new IntWritable();
		Configuration conf = context.getConfiguration();
		taskType = conf.get("TaskType");
		super.setup(context);
	}
	
		
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String []strs = value.toString().split("\\|");
		if(strs[0].equals("NA") || strs[1].equals("NA"))
			return;
		if(taskType.equals("2")){
			keyOutput.set(strs[0]);
			outputVal.set(1); // get one valid input 
			context.write(keyOutput, outputVal);	
		}
		else{ //task 3
			if(strs[0].equals("Onida")){
				keyOutput.set(strs[3]);
				outputVal.set(1); // get one valid input 
				context.write(keyOutput, outputVal);
			}
		}
	}

}
