/*
	Palash Nemade
*/

package edu.uta.cse6331;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

class Vertex implements Writable {

	// 0 for a graph vertex, 1 for a group number
	private int tag;
	// the group where this vertex belongs to
	private long group;
	// the vertex ID
	private long vID;
	// the vertex neighbors
	private ArrayList<Long> adjacent;
	
	public int getTag() {
		return tag;
	}

	public void setTag(int tag) {
		this.tag = tag;
	}

	public long getGroup() {
		return group;
	}

	public void setGroup(long group) {
		this.group = group;
	}

	public long getvID() {
		return vID;
	}

	public void setvID(long vID) {
		this.vID = vID;
	}

	public ArrayList<Long> getAdjacent() {
		return adjacent;
	}

	public void setAdjacent(ArrayList<Long> adjacent) {
		this.adjacent = adjacent;
	}


	public Vertex() {
		
	}

	public Vertex(int tag, long group, long vID, ArrayList<Long> adjacent) {
		this.tag = tag;
		this.group = group;
		this.vID = vID;
		this.adjacent = adjacent;
	}

	public Vertex(int tag, long group) {
		this.tag = tag;
		this.group = group;
		this.vID = 0;
		this.adjacent = new ArrayList<>();
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		adjacent = new ArrayList<Long>();
		tag = arg0.readInt();
		group = arg0.readLong();
		vID = arg0.readLong();
		int adj_list_size = arg0.readInt();
		for(int i = 0; i < adj_list_size; i++) {
			long adjElement = arg0.readLong();
			adjacent.add(adjElement);
		}
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeInt(tag);
		arg0.writeLong(group);
		arg0.writeLong(vID);
		arg0.writeInt(adjacent.size());
		for(long adjElement: adjacent) {
			arg0.writeLong(adjElement);
		}
	}

	public String toString() {
		return tag + " " + group + " " + vID + " " + adjacent.toString();
	}

}

public class Graph {

	public static class Mapper_1 extends Mapper<Object, Text, LongWritable, Vertex> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String str[] = value.toString().split(",");
			//String[] node = str.split(",");

			long vID = Long.parseLong(str[0]);

			ArrayList<Long> adjacent = new ArrayList<Long>();
			for (int i = 1; i < str.length; i++) {
				adjacent.add(Long.parseLong(str[i]));
			}

			context.write(new LongWritable(vID), new Vertex(0, vID, vID, adjacent));

		}

	}
	
	public static class Mapper_2 extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {
		public void map(LongWritable key, Vertex values, Context context) throws IOException, InterruptedException{
			
		
			LongWritable k = new LongWritable(values.getvID());
			context.write(k, values);
			
			for(long vAdj: values.getAdjacent()) {
				context.write(new LongWritable(vAdj), new Vertex(1,values.getGroup()));
			}
			
		}
	}
	
	public static class Reducer_2 extends Reducer<LongWritable,Vertex,LongWritable,Vertex>{
		public void reduce(LongWritable key, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
		
			ArrayList<Long> adj = new ArrayList<Long>();
			long m = Long.MAX_VALUE;
			for(Vertex v: values) {
				if(v.getTag() == 0) {
					adj = v.getAdjacent();
				}
				m = Math.min(m, v.getGroup());
			}
			context.write(new LongWritable(m), new Vertex(0,m,key.get(),adj));
		}
		
	}
	
	public static class Mapper_3 extends Mapper<LongWritable,Vertex,LongWritable,LongWritable>{
		public void map(LongWritable key, Vertex values, Context context) throws IOException, InterruptedException {
			context.write(new LongWritable(values.getGroup()), new LongWritable(1));
		}
	}
	
	public static class Reducer_3 extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable>{
		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long m = 0;
			for(LongWritable v:values) {
				m = m+(v.get());
			}
			context.write(key, new LongWritable(m));
		}
	}
	
	public static void main(String[] args) throws Exception {

		Job job1 = Job.getInstance();
		job1.setJobName("Graph part1");
		job1.setJarByClass(Graph.class);

		FileInputFormat.setInputPaths(job1, args[0]);

		job1.setMapperClass(Mapper_1.class);
		//job1.setMapOutputKeyClass(LongWritable.class);
		//job1.setMapOutputValueClass(Vertex.class);
		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(Vertex.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		SequenceFileOutputFormat.setOutputPath(job1, new Path(args[1]+"/file0"));
		//FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
		
		
		for(int i = 0;i<5;i++) {
			Job job2 = Job.getInstance();
			job2.setJobName("Graph part2");
			job2.setJarByClass(Graph.class);
			
			job2.setMapperClass(Mapper_2.class);
			job2.setMapOutputKeyClass(LongWritable.class);
			job2.setMapOutputValueClass(Vertex.class);
			
			job2.setReducerClass(Reducer_2.class);
			
			job2.setOutputKeyClass(LongWritable.class);
			job2.setOutputValueClass(Vertex.class);

			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			SequenceFileInputFormat.addInputPath(job2, new Path(args[1]+"/file"+i));
			SequenceFileOutputFormat.setOutputPath(job2, new Path(args[1]+"/file"+(i+1)));
			job2.waitForCompletion(true);

		}
		
		Job job3 = Job.getInstance();
		job3.setJobName("Graph part3");
		job3.setJarByClass(Graph.class);

		job3.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job3, new Path(args[1]+"/file5"));
		
		job3.setMapperClass(Mapper_3.class);
		job3.setMapOutputKeyClass(LongWritable.class);
		job3.setMapOutputValueClass(LongWritable.class);
		
		job3.setReducerClass(Reducer_3.class);
		
		job3.setOutputKeyClass(LongWritable.class);
		job3.setOutputValueClass(LongWritable.class);

		
		FileOutputFormat.setOutputPath(job3, new Path(args[2]));
		job3.waitForCompletion(true);

	}
}