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
import java.net.URI;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;




public class CompanyAdd1 {
	//Map1 is a function to get register number as a key, company name as a value.
	//input for that function is CompanyHouse data. Every step map function takes one line.
	//output for that function is register number and company name as key-value pair.
	//Line number(LongWritable) CompanyHouse(Text)--->Key(Text) Value(Text)
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();//to convert each line to string.
			String [] words= line.split(",");//to split each word for each line by ",".

			try{
				context.write(new Text(words[1]), new Text(words[0]));//return key-value pair as output
			}
			catch (NumberFormatException e) {
				// TODO: handle exception
			}

		}
	}
	//Key(Text) Value(Text)---->Key(Text) Value(Text)
	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
		//Reduce1 is a method which combine result of Map1.
		//input is output from Map1. Each step Reduce1 function takes one key-value pair. 
		CompanyAdd1 c=new CompanyAdd1();
		public void reduce(Text key, Iterable<Text> values, Context context)//values is a list of values with same key. 
				throws IOException, InterruptedException {

			for( Text value : values){
				context.write(key, value);//return key-value pairs.

			}
		}
	}
	//(Mapper)Address of each Website(Text)  Content of each Website(Text)--->Register Number(Text) URL(Text)
	public static class Map2 extends Mapper<Text, Text, Text, Text> {
		//Map2 is a method which takes Common Crawl(Address and Content) data as a input.
		HashMap<String, String> compHouse=new HashMap<String, String>();//to store Register number(key) and company name(value) pairs respectively.
		HashMap<String, String> compHouse2=new HashMap<String, String>();//to store company name(key) register number(value) respectively.

		public void configure(JobConf job) throws IOException{

			AmazonS3 s3Client = new AmazonS3Client(new PropertiesCredentials(//to connect AmazonS3.
					CompanyAdd1.class.getResourceAsStream(
							"AwsCredentials.properties")));

			S3Object s3object = s3Client.getObject(new GetObjectRequest(//to get output file of Map1-Reduce1.
					"company-regnum", "output1/part-r-00000"));


			BufferedReader b=new BufferedReader(new InputStreamReader(s3object.getObjectContent()));//to read output from file.

			try {
				while (b.readLine() != null)
				{

					String line=b.readLine(); //to read each line.
					String splitarray[] = line.split("\""); //output format of first map-reduce is SequenceFileOutputFormat.
					//So we cannot split with " " , split with "
					String compName=splitarray[splitarray.length-2].replaceAll("[^0-9A-Za-z ]", "").replaceAll("[LIMITEDLTD]", ""); // compname is in second from the end because of SequenceFileFormat.
					//Delete all punctuations and LIMITED or LTD.
					String regnum=splitarray[1];
					compHouse.put(regnum, compName);//to add register number and company name into CompHouse(HashMap), when register number is given, to get company name.
					compHouse2.put(compName, regnum);//to add register number and company name into CompHouse2(HashMap), when company name is given, to get register number.
				}

			}

			catch(NullPointerException e) {

			}	   

		}
		//URL of each website(Text) Content of each website(Text)--->Register number(Text) and URL(Text) as pair.
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

			String content = value.toString();//to convert content to string.
			String addr = key.toString();//to convert address to string.
			String [] nameword; //each cell include words of one companyname.
			//To get substring of the address(domain address) of each website until "/".
			String realaddress="";
			for (int i=0; i<addr.length(); i=i+1){
				if(addr.substring(i)=="/"){
					realaddress=addr.substring(0, i);
				}
			}
			if(realaddress==""){
				realaddress=addr;
			}
			String companyname;//each company name in CompanyHouse data.
			int y=0;//increment.
			ArrayList<String> nameword2=new ArrayList<String>();//List of words of company names after eliminate unnecessary information(example: one character in name)
			String [] words = content.split(" ");//to split each word in content and add them into array.

			ArrayList<String>lastfour=new ArrayList<String>();//it is a array which includes four words before "LIMITED" or "LTD" in content.
			lastfour.add("");
			lastfour.add("");
			lastfour.add("");
			lastfour.add("");
			int increment=0;
			int index=0;
			for(String word : words){
				word.replaceAll("[^0-9A-Za-z]", "");//to delete all punctuations in content.
				if(word.length()==8){//register number is 8 digits.
					if(compHouse.containsKey(word)){//to check whether word is equal to register number or not.
						companyname=compHouse.get(word);//to get company name belongs to that register number.
						nameword=companyname.split(" ");//to get each word of that company name.
						//below codes is to eliminate words with one character.
						for(int k=1; k<nameword.length; k++){
							if(nameword[k].length()==1){
								y=y+1;
							}
							else{

								nameword2.set(k-y, nameword[k]);//set words after eliminating.
							}
						}
						try {
							//if address before "/" includes first word or second word of name, write it.
							//to get real website of company.
							if(realaddress.contains(nameword2.get(0)) | realaddress.contains(nameword2.get(1))){
								context.write(new Text(word), new Text(realaddress));
								break;
							}
							//if address after "/" does not includes first word or second word of name or register number, write it.
							//to eliminate websites which inform some details about companies. 
							else if(!(addr.substring(realaddress.length(), addr.length()).contains(nameword2.get(0)) | 
									addr.substring(realaddress.length(), addr.length()).contains(nameword2.get(1)) | 
									addr.substring(realaddress.length(), addr.length()).contains(word)))
								context.write(new Text(word), new Text(realaddress));
							break;
						}
						catch (IndexOutOfBoundsException e) {
							// TODO: handle exception
						}
					}
				}
				//if content does not include register number.Below codes explains that:
				//if content includes "LIMITED" or "LTD", then that means it could be website of a company.
				//So check one word before "LIMITED" or "LTD" whether it is company name or not, if it is not, then check two words before
				//"LIMITED" or "LTD" and so on.
				if(word.equals("LIMITED")|word.equals("LTD")){
					if(compHouse2.containsKey(lastfour.get(4))){
						if(realaddress.contains(lastfour.get(4))){
							context.write(new Text(compHouse2.get(lastfour.get(4))),new Text(realaddress));
							break;
						}
					}
					else if(compHouse2.containsKey(lastfour.get(3)+" "+lastfour.get(4))){
						if(realaddress.contains(lastfour.get(3)) | realaddress.contains(lastfour.get(4))){
							context.write(new Text(compHouse2.get(lastfour.get(3)+" "+lastfour.get(4))),new Text(realaddress));
							break;
						}
					}
					else if(compHouse2.containsKey(lastfour.get(2)+" "+lastfour.get(3)+" "+lastfour.get(4))){
						if(realaddress.contains(lastfour.get(2)) | realaddress.contains(lastfour.get(3)) | realaddress.contains(lastfour.get(4))){
							context.write(new Text(compHouse2.get(lastfour.get(2)+" "+lastfour.get(3)+" "+lastfour.get(4))),new Text(realaddress));   
							break;
						}
					}
					else if(compHouse2.containsKey(lastfour.get(1)+" "+lastfour.get(2)+" "+lastfour.get(3)+" "+lastfour.get(4))){
						if(realaddress.contains(lastfour.get(1)) | realaddress.contains(lastfour.get(2)) | realaddress.contains(lastfour.get(3)) | realaddress.contains(lastfour.get(4))){
							context.write(new Text(compHouse2.get(lastfour.get(1)+" "+lastfour.get(2)+" "+lastfour.get(3)+" "+lastfour.get(4))),new Text(realaddress));   
							break;
						}
					}
				}

				index=(increment % 4);//to set up four words before "LIMITED" or "LTD"
				lastfour.set(index, word);
				increment=increment+1;

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
		//job1.setOutputFormatClass(SequenceFileAsTextOutputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		//MultipleOutputs.addNamedOutput(job1, "deneme", TextOutputFormat.class, Text.class, Text.class);    
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		job1.setJarByClass(CompanyAdd1.class);



		job1.waitForCompletion(true);

		//job2.setInputFormatClass(TextInputFormat.class);
		job2.setInputFormatClass(SequenceFileInputFormat.class);


		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);



		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);


		//MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Map1.class);
		//MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Map2.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		//job1.setOutputFormatClass(SequenceFileOutputFormat.class);

		//FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		String segmentListFile = "s3n://aws-publicdatasets/common-crawl/parse-output/valid_segments.txt";

		FileSystem fs = FileSystem.get(new URI(segmentListFile), conf);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(segmentListFile))));

		String segmentId;

		while ((segmentId = reader.readLine()) != null) {
			String inputPath = "s3n://aws-publicdatasets/common-crawl/parse-output/segment/"+segmentId+"/textData-*";
			FileInputFormat.addInputPath(job2, new Path(inputPath));
		}




		job2.setJarByClass(CompanyAdd1.class);

		job2.waitForCompletion(true);
	}
}
