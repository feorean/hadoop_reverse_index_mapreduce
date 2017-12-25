package co.uk.khalidmammadov.hadoop;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;

public class ParserMap extends Mapper<Object, Text, Text, Text>{

private Text word = new Text();


private void setWord(JSONObject obj, String key,  Set<String> uniqueList) {
	
	String[] words = obj.has(key)?obj.getString(key).split("\\s|,"):null;
	
	   if (words != null) {
		   for (String val : words) {		 
			
			   if (val.trim().length() > 0) {
			   
				   uniqueList.add(val.trim().replaceAll("[\\W+|\\\"]",""));
				   
			   }
	   
		   }	
	   }
}

public void map(Object key, Text value, Context context
              ) throws IOException, InterruptedException {

	String filename = Context.MAP_INPUT_FILE;  
	String line = value.toString();	

	try { 
   
		if (line.length() > 5) {
		  
		  
		  int lastIdx = line.length()-((line.substring(line.length()-1))==","?1:0);
		  
		  
		  JSONObject obj = null;
		  try {
			  obj = new JSONObject(line.substring(0, lastIdx));
		  }
		  catch  (JSONException e) {
			  return; 
		  }
		  
		  
		  if (obj != null) {
			   
			   Set<String> strSet = new HashSet<String>();
			
			   if (!obj.has("keywords") || !obj.has("base")) {
				   return;
			   }
	
			   String[] keys = {"keywords", "description", "title"};
			   
			   for (String s: keys) {				   
				   setWord(obj, s, strSet);
			   }
			   
	 
			   if (!strSet.isEmpty()) {
			   
				   for (String w: strSet) {  
				   
					   word.set(w);	      
						
					   context.write(word, new Text(obj.getString("base")));
					   
				   }
			   }
			   
			   
			   
		  }
	  }
	
	} catch(StackOverflowError t) {
		
	    System.out.println("Caught "+t);
	    t.printStackTrace();
	    System.out.println("************************FILE "+filename);
	}
}

}
