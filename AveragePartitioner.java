package average_sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AveragePartitioner extends Partitioner<Text,SumCountPair> {
        @Override
        public int getPartition(Text key, SumCountPair value, int numReduceTasks) {
		// customize which <K ,V> will go to which reducer
        	return key.toString().length()%numReduceTasks;
	}
}
