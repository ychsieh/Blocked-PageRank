import java.util.*;
import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Node implements Writable,Comparable<Node> {
    int nodeID;
    double pageRank;
    ArrayList<Integer> outgoing;

    //For internal Hadoop purposes.
    public Node() {
		nodeID = -1;
		outgoing = new ArrayList<Integer>();
    }
    public Node( Node node){
    	nodeID = node.nodeID;
    	pageRank = node.pageRank;
    	outgoing = new ArrayList<Integer>(node.outgoing);
    }
    public Node(String value) throws IOException{
    	String[] pieces = value.split("\\s+");
		if(pieces.length < 2 || pieces.length > 3) 
		    throw new IOException("Error Record: " + value.toString()); 
		
		this.nodeID = Integer.parseInt(pieces[0]);
		
		this.pageRank  = Double.parseDouble(pieces[1]);
		
		this.outgoing = new ArrayList<Integer>();
		
		if(pieces.length == 3) { 
		    String[] outNodes = pieces[2].split(","); 
		    for(int i = 0; i < outNodes.length; i++) {
		    	outgoing.add( Integer.parseInt(outNodes[i].trim()));
		    }
		} 
			
    }
    //For  parse use, init with nid and pr;
    public Node(int nid, double pr){
    	nodeID = nid;
    	pageRank = pr;
    	outgoing = new ArrayList<Integer>();
    }
    public static boolean isNode(String type){
		return type.equals("ND"); 
	}
    // add a outgoing edge to node
    public void add_edge(int v){
		outgoing.add(v);
		return ;
	}
    
    //Get the number of outgoing edges
    public int outgoingSize() {
    	return outgoing.size();
    }
    
    
    //Get the PageRank of this node.
    public double getPageRank() {
    	return pageRank;
    }
    
    //Set the PageRank of this node
    public void setPageRank(double pr) {
    	pageRank = pr;
    }

    //Used for internal Hadoop purposes.
    //Describes how to write this node across a network
    public void write(DataOutput out) throws IOException {
		out.writeInt(nodeID);
		out.writeDouble(pageRank);    
		for(int n : outgoing) {
		    out.writeInt(n);
		}
		out.writeInt(-1);
    }

    //Used for internal Hadoop purposes
    //Describes how to read this node from across a network
    public void readFields(DataInput in) throws IOException {
		nodeID = in.readInt();
		pageRank = in.readDouble();
		int next = in.readInt();
		outgoing = new ArrayList<Integer>();
		while (next != -1) {
			outgoing.add(next);
		    next = in.readInt();
		}
    }
    //get blockId by the Nodeid
    public static int blockIDofNode(int id){
    	if(Utility.random){
    		return Utility.Hash(id) % 68;
    	}else{
			int[] BlockIDs ={10328,20373,30629,40645,50462,60841,70591,80118,90497,100501,110567,120945,130999,140574,150953,161332,171154,181514,191625,202004,212383,222762,232593,242878,252938,263149,273210,283473,293255,303043,313370,323522,333883,343663,353645,363929,374236,384554,394929,404712,414617,424747,434707,444489,454285,464398,474196,484050,493968,503752,514131,524510,534709,545088,555467,565846,576225,586604,596585,606367,616148,626448,636240,646022,655804,665666,675448,685230};
			int l = 0, r =67;
			int mid=0;
			while(l<r){
				mid = (l+r)/2;
				if(BlockIDs[mid]<=id){
					l = mid+1;
				}else {
					r = mid;
				}
			}
			return l;
		}
	}
    //Gives a human-readable representaton of the node.
    public String toString() {
		String retv = "Node {\n";
		retv += "\tnodeid: " + nodeID + "\n";
		retv += "\tpageRank: " + pageRank + "\n";
		retv += "\toutgoing: ";
		String out = "";
		for(int n : outgoing) out += "" + n + ",";
		if(!out.equals("")) out = out.substring(0, out.length() - 1);
		retv += out + "\n";
		retv +="}";
		return retv;
    }
    public Text toMPText(){
		String retv="";
		retv+=nodeID;
		retv+=' ';
		retv+=Double.toString(pageRank);
		retv+=' ';
		String out = "";
		for(int n : outgoing) out += n + ",";
		if(!out.equals("")) out = out.substring(0, out.length() - 1);
		retv += out;
		return new Text(retv);
	}
    @Override
	public int compareTo(Node node) {
		// TODO Auto-generated method stub
		return this.nodeID-node.nodeID;
	}
}
