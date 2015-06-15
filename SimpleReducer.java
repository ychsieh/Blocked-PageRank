import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimpleReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
	public static double alpha = Utility.alpha;
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
	throws IOException, InterruptedException {
		//simply add pagerank to this node
        Node node = null; 
        double s = 0;
        double oldPageRank = 0;
        for (Text v : values){
            if  (Node.isNode(Utility.getType(v))) {
            	node =new Node(Utility.getValue(v));
            	oldPageRank = node.pageRank;
            }
	        else s += Double.parseDouble(Utility.getValue(v));
        }
        //deal with sink pagerank
        long G = Utility.NUM_OF_NODES;
        long m0000 = Long.parseLong(context.getConfiguration().get("sinkPageRank"));
        double m = m0000/Utility.POWER;
        double PR = m/G;
        if (!Utility.SINK_MODE){
        	PR=0;
        }
        node.setPageRank((1-alpha)/G + alpha*(PR + s)); 
        //add sink pagerank to counter
        if(node.outgoingSize()==0){
       	 long PR0000 = (long) (node.pageRank*Utility.POWER);
            context.getCounter(HadoopCounter.COUNTER.SINKPAGERANK).increment(PR0000);
       }
       //add residual to counter
        long PR00000 = (long) (Utility.computeResidual(oldPageRank, node.pageRank)*Utility.POWER);
        context.getCounter(HadoopCounter.COUNTER.RESIDUALS).increment(PR00000);
        
        context.write(null, node.toMPText()); 
    }
}
