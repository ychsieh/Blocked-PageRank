import java.io.IOException;

class Edge{
	int fromID;//edge from nodeid 
	int toID;//edge to nodeid 
	double pageRank;//edge pagerank
	boolean isBoundary;//if boundary edge 
	public Edge(String type,String value) throws IOException{
		//BC is boundary edge
		//BE in block edge
		switch (type){
			case "BC": isBoundary = true; break;
			case "BE": isBoundary = false; break;
			default:
				System.out.println("ERROR type"+type);
				break;
		}
		String[] pieces = value.split("\\s+");
		if(pieces.length < 2 || pieces.length > 3) 
		    throw new IOException("Error Record: " + value.toString()); 
		this.fromID = Integer.parseInt(pieces[0]);
		this.toID  = Integer.parseInt(pieces[1]);
		if (pieces.length==3)
			this.pageRank = Double.parseDouble(pieces[2]);
	}
}