import java.io.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
	
	private Text result = new Text();
	
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  String children = "";
	  double trans;
	  double sum = 0.0;
	  for(Text value : values) {
		  String line = value.toString();
		  	if(line.startsWith("0")) {
		  		trans = Double.parseDouble(line);
		  		sum += trans;
		   } else {
			  children = line;
		   }
	  }
	  children += Double.toString(sum);
	  result = new Text(children);
	  context.write(key, result);
  }
}