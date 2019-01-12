package average_sort;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, SumCountPair,Text,Double> {
	
    public void reduce(Text key, Iterable<SumCountPair> values, Context context) throws IOException, InterruptedException {
		// compute the average of same word
    	int sum = 0;
		int count = 0;
		for (SumCountPair val: values) {
			//TODO: agrregate the result from mapper
			sum = sum+val.getSum();
			count = count+val.getCount();
		}
		// write the result
		Text K=key;
		Double result = (double)sum/(double)count;
		context.write(key,result);
	}
}
