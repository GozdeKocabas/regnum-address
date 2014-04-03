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
	       
	
			
     		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	           String line = value.toString();
	           String [] words= line.split(","); //every component seperate with ","

	            
	           	try{
	        	   context.write(new Text(words[1]), new Text(words[0]));
	        	   //first word is company name(value), second word is register number(key)
	           	}
	           	catch (NumberFormatException e) {
					// TODO: handle exception
				}
	           	
	           	}
	    }
	
	 public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
	    	
	
	    	public void reduce(Text key, Iterable<Text> values, Context context) 
	          throws IOException, InterruptedException {
	    		
	    		for( Text value : values){
	    		context.write(key, value);
	    		
	    		}
	    	}
	 }
	
	
	 
	 
	 public static class Map2 extends Mapper<Text, Text, Text, Text> {
		
		HashMap<String, String> compHouse=new HashMap<String, String>();
	      
		public void configure(JobConf job) throws IOException{
      	   
      	   	//this part is to store output of first map-reduce in compHouse(HashMap). It takes from AmazonS3.
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
                   String splitarray[] = line.split("\""); //output format of first map-reduce is SequenceFileOutputFormat. So we cannot split with " " , split with "
                   String compName=splitarray[splitarray.length-2]; //compname is in second from the end 
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
	           String [] nameword; //each cell include words of one companyname.
	          
		   //   int registernum=0;
		      int control;
		      //String companyname;
		      String [] words = content.split(" "); //every website content splitted with " "
		       for(String word : words){
		    	   control= 0;
		    	   word.replaceAll("[^0-9A-Z]", ""); //remove everything except numbers and letters
		    	   if(word.length()== 8){ //register number is 8 digit
		    		   if(compHouse.containsKey(word)){
		    			   nameword=compHouse.get(word).split(" ");//sometimes address of company is not include exact name. For example if company name is "Is Bankası", but website address include just "Is". 
		    			   try{
		    			   if(addr.contains(nameword[0]) | addr.contains(nameword[1])){//We control first word or second word of companyname
		    				   context.write(new Text(word), key);// If it includes companyname, we think it is address of company.
		    				   break;
		    			   }
		    			   }
		    			   catch(NullPointerException e){
		    				   
		    			   }
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
	           
	       FileInputFormat.setInputPaths(job1, new Path(args[0]));//first argument is CompanyHouse data 
               job1.setOutputFormatClass(SequenceFileOutputFormat.class);
	       FileOutputFormat.setOutputPath(job1, new Path(args[2]));//first map-reduce's output path
	       
	       job1.setJarByClass(CompanyAdd1.class);
	       
	       
	       
	       job1.waitForCompletion(true);
	    
	       
	       
	       
	       
	     
	       job2.setInputFormatClass(SequenceFileInputFormat.class);
	       
	       
	       job2.setOutputKeyClass(Text.class);
	       job2.setOutputValueClass(Text.class);
	       
	       
	           
	       job2.setMapperClass(Map2.class);
	       job2.setReducerClass(Reduce2.class);
	           
	       FileInputFormat.setInputPaths(job2, new Path(args[1])); //Common Crawl Data(In Text Format)
	       job2.setOutputFormatClass(TextOutputFormat.class);
	       //job1.setOutputFormatClass(SequenceFileOutputFormat.class);
	           
	       //FileInputFormat.addInputPath(job, new Path(args[0]));
	       FileOutputFormat.setOutputPath(job2, new Path(args[3]));// second map-reduce's output path
	       
	       job2.setJarByClass(CompanyAdd1.class);
	       
	       job2.waitForCompletion(true);
	    
	       
	    	       
	    }
	
	
	
}
