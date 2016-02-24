import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {

  public static void main(String[] args) throws Exception {
	  
    /*
     * Validate that two arguments were passed from the command line.
     */
    if (args.length != 2) {
      System.out.printf("Usage: PageRank <input dir> <output dir>\n");
      System.exit(-1);
    }
    	  
	  int cnt = 0;
	  Path inPath = new Path(args[0]);
	  Path basePath = new Path(args[1] + "_iterations");
	  Path outPath = new Path (basePath, cnt + "");
	  
	  while(cnt < 3) {

	    @SuppressWarnings("deprecation")
		Job job = new Job();
	    job.setJarByClass(PageRank.class);
	    job.setJobName("PageRank");
	
	    if(cnt == 0) {
	    	FileInputFormat.addInputPath(job, inPath);
	    	FileOutputFormat.setOutputPath(job, outPath);
	    } else {
	    	inPath = outPath;
	    	outPath = new Path(basePath, cnt + "");
	    	FileInputFormat.addInputPath(job, inPath);
	    	FileOutputFormat.setOutputPath(job, outPath);
	    }
	    
	    job.setMapperClass(PageRankMapper.class);
	    job.setReducerClass(PageRankReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    /*
	     * Start the MapReduce job and wait for it to finish.
	     * If it finishes successfully, return 0. If not, return 1.
	     */
	    job.waitForCompletion(true);
	    cnt++;
	  }
	  System.exit(0);
  }
}

