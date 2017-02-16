package relative.frequencies;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class RelativeFrequency extends Configured implements Tool {
	//private static int distinct_words = 0;
	   public static void main(String[] args) throws Exception {
	      //System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new RelativeFrequency(), args);
	      
	     
	      //System.out.println(distinct_words);
	      System.exit(res);
	   }

	   @Override
	   public int run(String[] args) throws Exception {
	      //System.out.println(Arrays.toString(args));
		   //Configuration conf = new Configuration();
		   //conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true); 
		   //conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, GzipCodec.class,
			//	  CompressionCodec.class); // for setting the compression for the configuration
	      Job myjob = Job.getInstance(getConf());
	      myjob.setJarByClass(RelativeFrequency.class);
	      myjob.setOutputKeyClass(Text.class);
	      //myjob.setOutputValueClass(Text.class); //for stripes approach
	      myjob.setOutputValueClass(DoubleWritable.class);
	      
	     // myjob.setMapperClass(MapStripes.class); //for stripes approach
	      myjob.setMapperClass(MapPairs.class);
	      //myjob.setCombinerClass(ReduceStripes.class); //for setting combiner class
	      myjob.setNumReduceTasks(1); //my addition
	      //myjob.setReducerClass(ReduceStripes.class); //for stripes approach
	      myjob.setReducerClass(ReducePairs.class);
	      myjob.setInputFormatClass(TextInputFormat.class);
	      
	      myjob.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(myjob, new Path(args[0]));
	      FileOutputFormat.setOutputPath(myjob, new Path(args[1]));
	     
	      //FileOutputFormat.
	      myjob.waitForCompletion(true);
	     
	     //
	     FileSystem fs = FileSystem.get(getConf());  
	     RemoteIterator<LocatedFileStatus> direc = fs.listFiles(new Path(args[1]), false);
	   
	     
	     String output_path = "stopwords.csv";
	     FSDataOutputStream out = fs.create(new Path(output_path));
	     while(direc.hasNext()) {
	     Path temp_path = direc.next().getPath();
	     
	     if(temp_path.toString().contains("part")) {
	     
	     BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(temp_path)));
	     try {
	    	  String line;
	    	  line=br.readLine();
	    	  while (line != null){
	    		 String[] linesplit = line.split("\\s+");
			     String output  = linesplit[0] + ", " + linesplit[1] + "\n";
			     
	    	     out.writeBytes(output);//(output);

	    	    
	    	    line = br.readLine();
	    	  }
	    	} finally {
	    	  
	    	  br.close();
	    	}
	     }
	     
	     }
	     out.close();
	      return 0;
	   }
	   
	   public static class MapStripes extends Mapper<LongWritable, Text, Text, Text> {
		   //private final static IntWritable ONE = new IntWritable(1);
		    private Text wordkey = new Text();
		    
		    private Text wordvals = new Text();
		    private LinkedList<String> swords;
		    
		 
		    @Override
		    protected void setup(Context context) throws IOException, InterruptedException {
		    	 FileSystem fs = FileSystem.get(context.getConfiguration());  
		    	 swords = new LinkedList<String>(); 
		    	    String sw = fs.getHomeDirectory().toString() + "/stopwords.csv";
		    	   // System.out.println("Path in Mapper class is: " + sw);
		    	   try{
		    	    
		    	    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(sw))));
		    	    try {
		    	   	  String line;
		    	   	  
		    	   	  line=br.readLine();
		    	   	
		    	   	  while (line != null){
		    	   		//System.out.println(line);
		    	   		 String[] linesplit = line.split(",");
		    	   	     //System.out.println(linesplit[0].substring(0, linesplit[0].length() -1));
		    			 //swords.add(linesplit[0].substring(0, linesplit[0].length() -1));
		    	   		 //System.out.println(linesplit[0]);
		    	   		 swords.add(linesplit[0]);
		    			//System.out.println(linesplit[0]);
		    			 
		    			 line = br.readLine();
		    	   	  }
		    	   	} finally {
		    	   	  
		    	   	  br.close();
		    	   	}
		    	    }catch(IOException e) {
		    	    	System.out.println(e.toString());
		    	    }	  
		    }
		    
		    
		    @Override
		    public void map(LongWritable key, Text value, Context context)
		            throws IOException, InterruptedException {
		  	  
		    	LinkedList<String> linewords = new LinkedList<String>();
		       for (String token: value.toString().split("\\s+|--")) {
		          token = token.toLowerCase();
		          int end_index = token.length();
		          int i = 0;
		          //System.out.println("Token prior to processing is: " + token);
		          while(i < end_index) {
		          	//
		          	Character c = token.charAt(i);
		          	
		          	if(!Character.isLetterOrDigit(c)) { // not a letter or number
		          		if(i == 0) { //first character in string
		          			//if(c != '\'' || token.charAt(token.length()-1) == '\'') { //first letter is apostrophe, but not being used as quote (for Mark Twain's slang)
		          			token = token.substring(1, token.length());
		          			i--;
		          			//}
		          		}
		          		else if (i == token.length() -1 ) {
		          			token = token.substring(0, token.length()-1);
		          		}
		          		else {
		          			if(c != '-' && c != '\'') {
		          				token = token.substring(0, i) + token.substring(i+1, token.length());
		          				i--;
		          			}	
		          		}
		          		
		          	}
		          	i++;
		          	end_index = token.length();
		          }
		          
		          if(swords == null) {
		        	  System.out.println("Stop words is empty!");
		        	  System.exit(1);
		          }
		          //System.out.println("Token after processing is: " + token);
		          if(!token.isEmpty() && !swords.contains(token)) {
		          	linewords.add(token);
		          	
		          }
		       }
		       
		       for(int i = 0; i < linewords.size(); i++) {
		    	   
		    	   wordkey.set(linewords.get(i));
		    	   String vals = "";
		    	   for(int j = 0; j < linewords.size(); j++) {
		    		   if(j != i) {
		    			   vals = vals + linewords.get(j) + "\t";
		    		   }
		    	   }
		    	   if(!vals.isEmpty()) {
		    		   wordvals.set(vals);
		    		   context.write(wordkey, wordvals);
		    	   }
		       }
		    }
	   }
	   
	   
	   public static class MapPairs extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		   private final static DoubleWritable ONE = new DoubleWritable(1);
		    private LinkedList<String> swords;
		    
		 
		    @Override
		    protected void setup(Context context) throws IOException, InterruptedException {
		    	 FileSystem fs = FileSystem.get(context.getConfiguration());  
		    	 swords = new LinkedList<String>(); 
		    	    String sw = fs.getHomeDirectory().toString() + "/stopwords.csv";
		    	   // System.out.println("Path in Mapper class is: " + sw);
		    	   try{
		    	    
		    	    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(sw))));
		    	    try {
		    	   	  String line;
		    	   	  
		    	   	  line=br.readLine();
		    	   	
		    	   	  while (line != null){
		    	   		//System.out.println(line);
		    	   		 String[] linesplit = line.split(",");
		    	   	     //System.out.println(linesplit[0].substring(0, linesplit[0].length() -1));
		    			 //swords.add(linesplit[0].substring(0, linesplit[0].length() -1));
		    	   		 //System.out.println(linesplit[0]);
		    	   		 swords.add(linesplit[0]);
		    			//System.out.println(linesplit[0]);
		    			 
		    			 line = br.readLine();
		    	   	  }
		    	   	} finally {
		    	   	  
		    	   	  br.close();
		    	   	}
		    	    }catch(IOException e) {
		    	    	System.out.println(e.toString());
		    	    }	  
		    }
		    
		    
		    @Override
		    public void map(LongWritable key, Text value, Context context)
		            throws IOException, InterruptedException {
		  	  
		    	LinkedList<String> linewords = new LinkedList<String>();
		       for (String token: value.toString().split("\\s+|--")) {
		          token = token.toLowerCase();
		          int end_index = token.length();
		          int i = 0;
		          //System.out.println("Token prior to processing is: " + token);
		          while(i < end_index) {
		          	//
		          	Character c = token.charAt(i);
		          	
		          	if(!Character.isLetterOrDigit(c)) { // not a letter or number
		          		if(i == 0) { //first character in string
		          			//if(c != '\'' || token.charAt(token.length()-1) == '\'') { //first letter is apostrophe, but not being used as quote (for Mark Twain's slang)
		          			token = token.substring(1, token.length());
		          			i--;
		          			//}
		          		}
		          		else if (i == token.length() -1 ) {
		          			token = token.substring(0, token.length()-1);
		          		}
		          		else {
		          			if(c != '-' && c != '\'') {
		          				token = token.substring(0, i) + token.substring(i+1, token.length());
		          				i--;
		          			}	
		          		}
		          		
		          	}
		          	i++;
		          	end_index = token.length();
		          }
		          
		          if(swords == null) {
		        	  System.out.println("Stop words is empty!");
		        	  System.exit(1);
		          }
		          //System.out.println("Token after processing is: " + token);
		          if(!token.isEmpty() && !swords.contains(token)) {
		          	linewords.add(token);
		          	
		          }
		       }
		       
		       for(int i = 0; i < linewords.size(); i++) {
		    	   
		    	   //wordkey.set(linewords.get(i));
		    	   String key1 = linewords.get(i);
		    	   for(int j = 0; j < linewords.size(); j++) {
		    		   String key2 = linewords.get(j);
		    		   if(j != i && !linewords.get(j).isEmpty()) {
		    			   context.write(new Text(key1 + "\t" + key2), ONE);
		    		   }
		    	   }
		    	   
		       }
		    }
	   }
	   
	   
	   
	   
	   public static class ReduceStripes extends Reducer<Text, Text, Text, Text> {
		   private double[] maxvals = new double[100];
		   //private int counter = 0;
		   private String[] finaloutput = new String[100];
		   private DecimalFormat df = new DecimalFormat("0.00000");
		   @Override
	    public void reduce(Text key, Iterable<Text> values, Context context)
	            throws IOException, InterruptedException {
			   int denom = 0;
			   HashMap<String, Integer> newvals = new HashMap<String, Integer>();
			   for(Text v : values) {
				   String[] t = v.toString().split("\\s+");
				   for(int i = 0; i < t.length; i++) {
					   if(!newvals.containsKey(t[i])) {
						   newvals.put(t[i], 1);
					   }
					   else {
						   newvals.put(t[i], 1+ newvals.get(t[i]));
					   }
					   denom++;
				   }
			   }
			   
			   for(String k : newvals.keySet()) {
				   double fr = ((double) newvals.get(k))/((double) denom);
				   /*if(counter < maxvals.length) {
					   int c = 0;
					   while(c < counter +1 && fr > maxvals[c]) {
						   c++;
					   }
					   double torepd = fr;
					   String toreps = key + ", " + k;
					   for(int i = c; i < counter+1; i++) {
						   double tempd = maxvals[i];
						   String temps = finaloutput[i];
						   maxvals[i] = torepd;
						   finaloutput[i] = toreps;
						   torepd = tempd;
						   toreps = String.valueOf(temps);
					   }
				   
					   counter++;
			   }   
				   else{*/
					  if(denom > 50 && Double.compare(fr, maxvals[0]) > 0) {
						  int c = 1;
						  while(c < maxvals.length && Double.compare(fr, maxvals[c]) > 0) {
							  c++;
						  }
						  c = c-1;
						  double torepd = fr;
						  String toreps = key + ", "  + k;
						  for(int i = c; i > -1; i--) {
							   double tempd = maxvals[i];
							   String temps = finaloutput[i];
							   maxvals[i] = torepd;
							   finaloutput[i] = toreps;
							   torepd = tempd;
							   toreps = String.valueOf(temps);
						   }
					  }
				   //}
				   
			   }
			   
		   }
		   @Override
		   protected void cleanup(Context context) throws IOException,
           InterruptedException{
			   for(int i = finaloutput.length-1; i > -1; i--) {
				   context.write(new Text(finaloutput[i]), new Text(df.format(maxvals[i])));
			   }
		   }
		   
		}
	   
	   public static class ReducePairs extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		   private double[] maxvals = new double[100];
		   //private int counter = 0;
		   private String[] finaloutput = new String[100];
		   private HashMap<String, Double> denoms = new HashMap<String, Double>();
		   private HashMap<String, Double> nums = new HashMap<String, Double>();
		   //private DecimalFormat df = new DecimalFormat("0.00000");
		   @Override
	    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
	            throws IOException, InterruptedException {
			   double d = 0;
			   for(DoubleWritable v : values){
				   d+= v.get();
			   }
			   nums.put(key.toString(), d);
			   String[] keys2 = key.toString().split("\\s+");
			   if(!denoms.containsKey(keys2[0])) {
				   denoms.put(keys2[0], d);
			   }
			   else {
				   denoms.put(keys2[0], denoms.get(keys2[0]) + d); 
			   }
			   
		   }
		   @Override
		   protected void cleanup(Context context) throws IOException,
           InterruptedException{
			   
			   
			   for(String k : nums.keySet()) {
				   String dnum1 = k.split("\\s+")[0];
				   double denom  = denoms.get(dnum1);
				   double fr = nums.get(k)/denom;
				   /*if(counter < maxvals.length) {
					   int c = 0;
					   while(c < counter +1 && fr > maxvals[c]) {
						   c++;
					   }
					   double torepd = fr;
					   String toreps = key + ", " + k;
					   for(int i = c; i < counter+1; i++) {
						   double tempd = maxvals[i];
						   String temps = finaloutput[i];
						   maxvals[i] = torepd;
						   finaloutput[i] = toreps;
						   torepd = tempd;
						   toreps = String.valueOf(temps);
					   }
				   
					   counter++;
			   }   
				   else{*/
					  if(denom > 50 && Double.compare(fr, maxvals[0]) > 0) {
						  int c = 1;
						  while(c < maxvals.length && Double.compare(fr, maxvals[c]) > 0) {
							  c++;
						  }
						  c = c-1;
						  double torepd = fr;
						  String toreps = dnum1 + ", " + k.split("\\s+")[1];
						  for(int i = c; i > -1; i--) {
							   double tempd = maxvals[i];
							   String temps = finaloutput[i];
							   maxvals[i] = torepd;
							   finaloutput[i] = toreps;
							   torepd = tempd;
							   toreps = String.valueOf(temps);
						   }
					  }
			   }
			   
			   for(int i = finaloutput.length-1; i > -1; i--) {
				   context.write(new Text(finaloutput[i]), new DoubleWritable(maxvals[i]));
			   }
		   }
		   
		}
	   
}
