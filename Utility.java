import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.io.Text;


public class Utility {
	//public static final int  NUM_OF_NODES = 5;
	public static final double alpha=0.85;   // damping factor
	public static final boolean SIMPLE_MODE = false; //true: use simple pagerank     false: use blocked pagerank
	public static final long  NUM_OF_NODES = 685230L;// Number of nodes
	public static final long NUM_OF_GROUPS = 68L;// Number of Groups
	public static final boolean PARSEINPUT= false; //if Parse Input from edge information to node information
	public static final boolean REJECTBYNETID = true; // if filter the edge by netid 
	public static int NUM_OF_SINK_NODE=0; // Count number of sink node in input parse
	public static final boolean DEBUG = false;  //if output debug msg
	public static final boolean GAUSS_SEIDEL =true; //if use GAUSS_SEIDEL
	public static final boolean SINK_MODE = false; //if Add Back sink node pagerank
	public static final boolean PRINT_LEAST_2_NODE = true;//if print lease 2node every group
	public static final String inputFile = "edges.txt"; //InputFile of edge information
	public static final String parserOutputFile = "input/input.txt"; // parse result output file
	public static final String outputFile = "out.txt"; //final stats output file
	public static final double POWER = 1000000000.0; //Counter precision 10^9
	public static final int INBLOCK_ITERATION_LIMIT = 20; // in block iteration limit
	public static final double REJECT_MIN = 0.5886; //edge reject min by zl456
	public static final double REJECT_LIMIT = 0.5986;//edge reject max by zl456
	public static int EDGE_CNT=0;// edge count calculated in the parse function
	public static final boolean random = false; // if use random partition
	
	//compute residual
	public static double computeResidual(double oldVal,double newVal){
		return Math.abs(oldVal-newVal)/newVal;
	}
	//get type of map reduce message
	public static String getValue(Text values){
		String tmpStr = values.toString();
		return tmpStr.substring(tmpStr.indexOf(' ')+1);
	}
	//get value of map reduce message
	public static String getType(Text values){
		String tmpStr = values.toString();
		String type =  tmpStr.substring(0,tmpStr.indexOf(' '));
		return type;
	}
	//random partition hash function
	public static int Hash(int val){
		int hashCode =val;
		hashCode ^= (hashCode>>>20)^(hashCode>>>12);
		hashCode ^=(hashCode>>>7)^(hashCode>>>4);
		return hashCode;
	}
	//write least 2 node each block by reading the final round mapreduce output file 
	public static void writeLeast(int round,PrintWriter writer) throws NumberFormatException, IOException{
		double first[]=new double[68];
		double second[]=new double[68];
		int firstId[]=new int[68];
		int secondId[]=new int[68];
		for(int i =0;i< 68;i++){
			firstId[i]=(int)Utility.NUM_OF_NODES;
			secondId[i]=(int)Utility.NUM_OF_NODES;
		}
		String finalOutputFile =  "stage"+round+"/part-r-00000";
		System.out.println(finalOutputFile);
		@SuppressWarnings("resource")
		BufferedReader buffer=new BufferedReader(new FileReader(finalOutputFile));
		String line;
		 while ((line=buffer.readLine()) != null) {
			String[] pieces = line.split("\\s+");
			
			int nodeId = Integer.parseInt(pieces[0]);
			int blockId = Node.blockIDofNode(nodeId);
			double pageRank = Double.parseDouble(pieces[1]);
			if(nodeId>secondId[blockId]) continue;
			if(nodeId<firstId[blockId]){
				secondId[blockId] = firstId[blockId];
				second[blockId] = first[blockId];
				firstId[blockId] = nodeId;
				first[blockId] = pageRank;
			}else
			if(nodeId<secondId[blockId]){
				secondId[blockId] = nodeId;
				second[blockId] = pageRank;
			}
	     }   
		 for(int i = 0 ;i<68;i++){
			 writer.println("Group:"+i+" firstId:"+firstId[i]+", "+first[i]+" ;    secondId:"+secondId[i]+", "+second[i]);
		 }
	}
}
