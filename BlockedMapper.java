import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


public class BlockedMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException 
	{
		// build a Node instance to retrieve the blockID of the node
		Node curNode = new Node(value.toString());
		int blockID = Node.blockIDofNode(curNode.nodeID);
		
		// emit the key-value pair as <block(𝑢); u, PR𝑡(𝑢), {〈block(𝑣), 𝑣〉 | 𝑢 → 𝑣> }> 
		// to pass on the basic graph information for a specific block
		context.write(new IntWritable(blockID), new Text("ND "+value.toString()));
		
		// for every outgoing node v emit key-value pair 〈block(𝑣); v, block(𝑢), PR𝑡(𝑢)/deg(𝑢) |𝑢 → 𝑣〉 
		// to pass on edges information inside the block and the flow-in page rank of boundary nodes for a specific block  
    	for(int id:curNode.outgoing){
    		int toBlockID = Node.blockIDofNode(id);
			if(blockID == toBlockID ){
				String txtStr="BE "+curNode.nodeID+" "+id;
				context.write(new IntWritable(toBlockID), new Text(txtStr));
			}else{
				String txtStr="BC "+curNode.nodeID+" "+id+" "+Double.toString(curNode.pageRank/curNode.outgoingSize());
				context.write(new IntWritable(toBlockID), new Text(txtStr));
			}
    	}
	}
}
