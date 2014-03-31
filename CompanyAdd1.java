import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapred.machines_jsp;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.springframework.beans.TypeMismatchException;




import java.io.InputStreamReader;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;




public class CompanyAdd1 {

	
	
	

	
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
	       //private final static IntWritable one = new IntWritable(1);
	       
	
			
     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	           String line = value.toString();
	           String [] words= line.split(",");

	            //context.write(key, value);
	          //if(words[1].substring(0,1).equals("\""))
	           
	           //else
	           	try{
	        	   context.write(new Text(words[1]), new Text(words[0]));
	           	}
	           	catch (NumberFormatException e) {
					// TODO: handle exception
				}
	           	
	           	}
	    }
	
	 public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
	    	
		 CompanyAdd1 c=new CompanyAdd1();
	    	public void reduce(Text key, Iterable<Text> values, Context context) 
	          throws IOException, InterruptedException {
	    		
	    		for( Text value : values){
	    		context.write(key, value);
	    		
	    		}
	    	}
	 }
	
	
	 
	 
	 public static class Map2 extends Mapper<Text, Text, Text, Text> {
		
		 //String [] compname = new String[100000000];
		 HashMap<String, String> compHouse=new HashMap<String, String>();
	      
		 public void configure(JobConf job) throws IOException{
      	   
      	   AmazonS3 s3Client = new AmazonS3Client(new PropertiesCredentials(
		                CompanyAdd1.class.getResourceAsStream(
		                		"AwsCredentials.properties")));
		       
		       S3Object s3object = s3Client.getObject(new GetObjectRequest(
	           		"julienpro", "output1/part-r-00000"));
		       
		      
		      BufferedReader b=new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
		  	 
		  	 
		      try{
	           while (b.readLine() != null)
             {
	        	   
	        	     String line=b.readLine(); 
                   String splitarray[] = line.split("\"");
                   String compName=splitarray[splitarray.length-2];
                   String regnum=splitarray[1];
                   compHouse.put(regnum, compName);
                   
                   
             }
	           
	           }
	           
	           catch(NullPointerException e){
	        	   
	           }	   
      	   
         }
              
		 
		 
		 
		 
		 
		 
		 public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
	        	  
	        	  
	        	   
	           String content = value.toString();
	           String addr = key.toString();
	           String [] nameword;
	          
		   //   int registernum=0;
		      int control;
		      //String companyname;
		      String [] words = content.split(" ");
		       for(String word : words){
		    	   control= 0;
		    	   word.replaceAll("[^0-9A-Z]", "");
		    	   if(word.length()== 8){
		    		   if(compHouse.containsKey(word)){
		    			  // nameword=compHouse.get(word).split(" ");
		    			  // try{
		    			   //if(addr.contains(nameword[0]) | addr.contains(nameword[1])){
		    				   context.write(new Text(word), key);
		    				 //  break;
		    			   //}
		    			   //}
		    			   //catch(NullPointerException e){
		    				   
		    			   //}
		    			   }
		    			   
		    		   }
		    	   
		    	   }
		    	   
		    	   
		    		   
		    	   
		       }
	       }
	    
	       
	          
	 
	 
	 
	
	
	 public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
	    	
	    	public void reduce(Text key, Iterable<Text> values, Context context) 
	          throws IOException, InterruptedException {
	    		
	    		for( Text value : values){
	    		context.write(key, value);
	    		}
	    		
	    		
	    		
	    	}
	 }
	     
	    
	
	
	
	 public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
	
	           
	           Job job1 = new Job(conf, "job1");
	           Job job2 = new Job(conf, "job2");

	           
	           
	           
	       
job1.setOutputKeyClass(Text.class);
	   job1.setOutputValueClass(Text.class);
	           
	       job1.setMapperClass(Map1.class);
	       job1.setReducerClass(Reduce1.class);
	           
	       FileInputFormat.setInputPaths(job1, new Path(args[0]));
	       //MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Map1.class);
	       //MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Map2.class);
           // job1.setOutputFormatClass(SequenceFileAsTextOutputFormat.class);
     job1.setOutputFormatClass(SequenceFileOutputFormat.class);
	       //MultipleOutputs.addNamedOutput(job1, "deneme", TextOutputFormat.class, Text.class, Text.class);    
	       //FileInputFormat.addInputPath(job, new Path(args[0]));
	       FileOutputFormat.setOutputPath(job1, new Path(args[2]));
	       
	       job1.setJarByClass(CompanyAdd1.class);
	       
	       
	       
	       job1.waitForCompletion(true);
	    
	       
	       
	       
	       
	       //job2.setInputFormatClass(TextInputFormat.class);
	       job2.setInputFormatClass(SequenceFileInputFormat.class);
	       
	       
	       job2.setOutputKeyClass(Text.class);
	       job2.setOutputValueClass(Text.class);
	       
	       
	           
	       job2.setMapperClass(Map2.class);
	       job2.setReducerClass(Reduce2.class);
	           
	       FileInputFormat.setInputPaths(job2, new Path(args[1]));
	       //MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Map1.class);
	       //MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Map2.class);
	       job2.setOutputFormatClass(TextOutputFormat.class);
	       //job1.setOutputFormatClass(SequenceFileOutputFormat.class);
	           
	       //FileInputFormat.addInputPath(job, new Path(args[0]));
	       FileOutputFormat.setOutputPath(job2, new Path(args[3]));
	       
	       job2.setJarByClass(CompanyAdd1.class);
	       
	       job2.waitForCompletion(true);
	    
	       
	    	       
	    }
	
	
	
}
