package co.uk.khalidmammadov.hadoop;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReverseIndexReducer extends Reducer<Text,Text,Text,Text> {

 
	private Text result = new Text();

	public void reduce(Text key, Iterable<Text> values,
 				 Context context
              ) throws IOException, InterruptedException {
	 
		String locations = "";
	 
		Set<String> strSet = new HashSet<String>();
		
		//Remove duplicates
		for (Text val : values) {		 
			strSet.add(val.toString());
		}
		//Join URLs
		for (String str : strSet) {			 
			locations = String.join("|", locations, str);	 
		}
		
		//strSet.clear();
		
		result.set(locations);
	 
		//result.set(new Text("test"));
		context.write(key, result);
		
		//System.gc();
	}

}	
