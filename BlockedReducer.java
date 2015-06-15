import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class BlockedReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
	public static double alpha = Utility.alpha;
	public long N =Utility.NUM_OF_NODES;
	private ArrayList<Node> BlockNodeOrigin = new ArrayList<Node>();
	
	private HashMap<Integer,Node> BlockNodes = new HashMap<Integer,Node>();
	private HashMap<Integer,ArrayList<Edge>> edgesOfNode = new HashMap<Integer,ArrayList<Edge>>();
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		BlockNodeOrigin.clear();
		BlockNodes.clear();
		edgesOfNode.clear();
		ArrayList<Edge> edges = new ArrayList<Edge>();
		edges.clear();
		
		// retrieve and store information from values into local data structures
		for (Text txt:values){
			String type = Utility.getType(txt);
			String value = Utility.getValue(txt);
			if(Node.isNode(type)){
				Node curNode = new Node(value);
				BlockNodeOrigin.add(new Node(curNode));
				BlockNodes.put(curNode.nodeID,new Node(curNode));
			}else{
				Edge newEdge = new Edge(type,value);
				edges.add(newEdge);
				if(edgesOfNode.get(newEdge.toID) == null)
					edgesOfNode.put(newEdge.toID,new ArrayList<Edge>());
				ArrayList<Edge> newArr = edgesOfNode.get(newEdge.toID);
				newArr.add(newEdge);
			}
		}
		if(Utility.GAUSS_SEIDEL)Collections.sort(BlockNodeOrigin);
        long m0000 = Long.parseLong(context.getConfiguration().get("sinkPageRank"));
        double m = m0000/10000.0;
      
        // sink node redistribution
        double sinkPR=0;
        if(Utility.SINK_MODE){
        	sinkPR = m/N;
        }
        
        // iterate and update page rank inside the block 
        // and exit if block average residual is bigger than 0.001 or the number of iterations is bigger than INBLOCK_ITERATION_LIMIT
		double residual=1;
		int itr_cnt=0;
		while (residual>0.001&&(++itr_cnt<Utility.INBLOCK_ITERATION_LIMIT)){
			
			context.getCounter(HadoopCounter.COUNTER.ITERATIONS).increment(1);
			residual = IterateInBlockOnce(sinkPR);
		}
		
		//Calculate the total block residuals
		double finalResiduals =0;
		for(int i =0 ;i<BlockNodeOrigin.size();i++){
			int nid = BlockNodeOrigin.get(i).nodeID;
			double newPR=BlockNodes.get(nid).getPageRank();
			double oldPR=BlockNodeOrigin.get(i).pageRank;
			finalResiduals+=Utility.computeResidual(oldPR, newPR);
		}
		long PR00000 = (long) (finalResiduals*Utility.POWER);
	    context.getCounter(HadoopCounter.COUNTER.RESIDUALS).increment(PR00000);
	    //write block node to output
	    for(Node node : BlockNodeOrigin){
	    	context.write(null, BlockNodes.get(node.nodeID).toMPText()); 
	    }
	}
	// 
	private double IterateInBlockOnce(double sinkPR){
		HashMap<Integer, Double> newPR = new HashMap<Integer, Double>();
		newPR.clear();
		double residual=0;
		if(Utility.DEBUG)
		System.out.println("old");
		//init for block information
		for(Node node : BlockNodeOrigin){
			newPR.put(node.nodeID,0.0);
			if(Utility.DEBUG)
			System.out.println(BlockNodes.get(node.nodeID).toMPText());
		}
		//run in block iteration
		for(Node node : BlockNodeOrigin){
			ArrayList<Edge> edges = edgesOfNode.get(node.nodeID); 
			double newPageRank = 0;
			if(edges!=null)
			for(Edge e:edges){
				if(e.isBoundary){
					newPageRank+=e.pageRank;
				}else{
					Node curNode= BlockNodes.get(e.fromID);
					double addPR = curNode.pageRank /(double)curNode.outgoingSize();
					newPageRank+=addPR;
				}
			}
			Node curNode = BlockNodes.get(node.nodeID);
			double oldPageRank= curNode.getPageRank();
			newPageRank = alpha*(newPageRank) + (1-alpha)/(double)Utility.NUM_OF_NODES;
			newPR.put(node.nodeID, newPageRank);
			if(Utility.GAUSS_SEIDEL)
				BlockNodes.get(node.nodeID).setPageRank(newPR.get(node.nodeID));
			residual+=Utility.computeResidual(oldPageRank, newPageRank);
		}
		//collect ouput;
		for(Node node : BlockNodeOrigin){
			BlockNodes.get(node.nodeID).setPageRank(newPR.get(node.nodeID));
			if(Utility.DEBUG)
			System.out.println(BlockNodes.get(node.nodeID).toMPText());
		}
		if(Utility.DEBUG)
		System.out.println(residual);
		return residual/(double)BlockNodeOrigin.size();
	}

}
