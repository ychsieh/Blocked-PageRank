import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Scanner;


public class InputParser {

	public static boolean selectInputLine(double x) {
		return ( ((x >= Utility.REJECT_MIN) && (x < Utility.REJECT_LIMIT)) ? false : true );
	}
	
	public void parse(String inputFile, String outputFile) throws IOException {
	 	@SuppressWarnings("resource")
		Scanner sc = new Scanner(new FileReader(inputFile));
		PrintWriter writer = new PrintWriter(outputFile, "UTF-8");
		double rand;
	 	int u, v;
	 	//create nodes
	 	ArrayList<Node> Nodes = new ArrayList<Node>();
	 	for( int i = 0; i < Utility.NUM_OF_NODES; i++){
	 		Nodes.add(new Node(i,(double)(1.0/Utility.NUM_OF_NODES)));	
	 	}
	 	int cur = -1 ;
	 	Node curNode = null;
	 	while(sc.hasNext()){
	 		rand = sc.nextDouble();
	 		u = sc.nextInt();
	 		v = sc.nextInt();
	 		//check reject
	 		if(selectInputLine(rand)||!Utility.REJECTBYNETID){
	 			Utility.EDGE_CNT++;
		 		if(u>cur){
		 			cur = u;
		 			curNode = Nodes.get(cur);
		 		}
		 		curNode.add_edge(v);
	 		}
	 	}
	 	for(Node n : Nodes){
	 		if(n.outgoingSize()==0)Utility.NUM_OF_SINK_NODE=Utility.NUM_OF_SINK_NODE+1;
	 		writer.println(n.toMPText());
	 	}
		writer.close();
		// print end of parse
		System.out.println("end");
	}
}
