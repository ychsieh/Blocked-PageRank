
import java.io.PrintWriter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.log4j.*;

public class PageRank {

	@SuppressWarnings("resource")
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		String  baseFolder = "";
		int round=0;
		if(Utility.PARSEINPUT){
			InputParser parser = new InputParser();
			parser.parse(Utility.inputFile,Utility.parserOutputFile);
		}
		PrintWriter writer = new PrintWriter(Utility.outputFile, "UTF-8");
		long sinkPageRank = (long)((double)Utility.NUM_OF_SINK_NODE/Utility.NUM_OF_NODES*Utility.POWER);
		long residuals = 0;

		while (true){
			
			Configuration conf = new Configuration();
			conf.set("sinkPageRank", ""+sinkPageRank);
			conf.set("sink","0");
			Job job = Job.getInstance(conf, "Jobname");
			
			job.setJarByClass(PageRank.class);
			if(Utility.SIMPLE_MODE){
				job.setMapperClass(SimpleMapper.class);
				job.setReducerClass(SimpleReducer.class);
			}else {
				job.setMapperClass(BlockedMapper.class);
				job.setReducerClass(BlockedReducer.class);
			}
			// TODO: specify output types
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			
			// TODO: specify input and output DIRECTORIES (not files)
			String inputPath = round == 0 ? "input" : "stage" + (round-1);
		    String outputPath = "stage" + (round);
		    
			FileInputFormat.setInputPaths(job, new Path(baseFolder+inputPath));
			FileOutputFormat.setOutputPath(job, new Path(baseFolder+outputPath));
			
			try { 
				job.waitForCompletion(true);
		    } catch(Exception e) {
		    	System.err.println("ERROR IN JOB: " + e);
		    	return ;
		    }
			//caculates residuals iterations and output 
			Counters counters = job.getCounters(); 
			sinkPageRank = counters.findCounter(HadoopCounter.COUNTER.SINKPAGERANK).getValue();  
			residuals = counters.findCounter(HadoopCounter.COUNTER.RESIDUALS).getValue();
			long iterations =counters.findCounter(HadoopCounter.COUNTER.ITERATIONS).getValue();
			writer.println("round "+round+" residual is :"+(double)residuals/Utility.POWER/Utility.NUM_OF_NODES+"    the Avg Iterations is: "+(double)iterations/(double)Utility.NUM_OF_GROUPS);
			if(residuals/(double)Utility.NUM_OF_NODES/(double)Utility.POWER<0.001) break;//100000*0.001
			if(Utility.SIMPLE_MODE){
				if(round==6)break;
			}
			System.out.println("total residual is:"+residuals+"    the Avg Iterations is: "+iterations/Utility.NUM_OF_GROUPS);
			round++;
			
		}
		//check extra output
		if(Utility.PRINT_LEAST_2_NODE){
			writer.println("Group Least 2 Node PageRank");
			Utility.writeLeast(round, writer);
		}
		if(Utility.PARSEINPUT&&Utility.REJECTBYNETID){
			writer.println("Reject Min:"+Utility.REJECT_MIN+" ;Reject Max:"+Utility.REJECT_LIMIT);
			writer.println("Accepte Edge Count :"+Utility.EDGE_CNT);
		}
		writer.close();
		return ;
	}

}
