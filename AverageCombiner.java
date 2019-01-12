package average_sort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageCombiner extends Reducer<Text, SumCountPair, Text, SumCountPair> {
	// Combiner implements method in Reducer
	
	public void reduce(Text key, Iterable<SumCountPair> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		int count = 0;
		for (SumCountPair val: values) {
			//TODO: agrregate the result from mapper
			sum = sum+val.getSum();
			count = count+val.getCount();
		}
		Text K=key;
		SumCountPair V=new SumCountPair(sum,count);
		context.write(K,V);
	}
}
