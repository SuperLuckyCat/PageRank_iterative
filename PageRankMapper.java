import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Object, Text, Text, Text> {
	
	private double val = 0.0;
	private Text page = new Text();

  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {

	  String line = value.toString();
	  String[] tokens = line.split("\\s+");
	  val = (Double.parseDouble(tokens[tokens.length - 1])) / (tokens.length - 2);
	  String valString = Double.toString(val);
	  String children = "";
	  
	  for (int i = 1; i < (tokens.length - 1); i++) {
		  page.set(tokens[i]);
		  context.write(page, new Text(valString));
	  }
	  for (int i = 1; i < (tokens.length - 1); i++) {
		  children += (tokens[i] + " ");
	  }
	  page.set(tokens[0]);
	  context.write(page, new Text(children));
  }
}