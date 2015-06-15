import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SimpleMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	// two output type 
    	// ND stands for Node 
    	// PR stands for Page rank
        Node curNode = new Node(value.toString());	
        context.write(new IntWritable(curNode.nodeID), new Text("ND "+value.toString()));
        for (int id : curNode.outgoing){
        	double pageRank = (curNode.pageRank/curNode.outgoingSize());
        	context.write(new IntWritable(id), new Text("PR "+Double.toString(pageRank)));
        }
    }
}