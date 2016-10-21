/**
 * 
 */
package eu.unitn.disi.db.grava.utils;

/**
 * @author Zhaoyang
 *
 */
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.jayway.jsonpath.JsonPath;

import eu.unitn.disi.db.grava.graphs.Edge;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class Convertion {
  public static Properties properties;
  public static Map<Long, String> predMap;
  
  public Convertion() {
	  properties = new Properties();
	  predMap = null;
	  init();
  }
  public String toReadable(Collection<Edge> edgeSet) {
	  String results = "";
	  for (Edge e : edgeSet) {
		  String temp = toReadable(e);
		  if (temp == null) {
			  return null;
		  }
		  results += temp + "\n";
	  }
	  return results;
  }
  public String toReadable(Edge e){
	  String results = "";
	  String temp = mid2Readable(long2mid(e.getSource()));
//	  System.out.println(temp);
	  if (temp == null) {
		  return null;
	  }
	  results += temp;
	  results += " [" + predMap.get(e.getLabel());
	  
	  temp = mid2Readable(long2mid(e.getDestination()));
//	  System.out.println(temp + " " + long2mid(e.getDestination()));
	  if (temp == null) {
		  return null;
	  }
	  results += "] " + temp;
	  return results;
  }
  
  public void init() {
	  try {
		properties.load(new FileInputStream("freebase.properties"));
		predMap = new HashMap<>();
		BufferedReader br = new BufferedReader(new FileReader("predicatesWithID.txt"));
		String line = null;
		while((line = br.readLine()) != null){
			String[] words = line.split(" ");
			String[] wws = words[0].split("\\.");
			predMap.put(Long.valueOf(words[1]), wws[wws.length - 1]);
		}
		br.close();
	} catch (IOException e) {
		e.printStackTrace();
	}
  }
  public String mid2Readable(String mid) {
	  String re = null;
	  try {
	      
	      HttpTransport httpTransport = new NetHttpTransport();
	      HttpRequestFactory requestFactory = httpTransport.createRequestFactory();
	      JSONParser parser = new JSONParser();
	      GenericUrl url = new GenericUrl("https://www.googleapis.com/freebase/v1/search");
	      url.put("query", mid);
//	      url.put("filter", "(all type:/music/artist created:\"The Lady Killer\")");
//	      url.put("limit", "10");
	      url.put("indent", "true");
	      url.put("key", properties.get("API_KEY"));
	      HttpRequest request = requestFactory.buildGetRequest(url);
	      HttpResponse httpResponse = request.execute();
	      JSONObject response = (JSONObject)parser.parse(httpResponse.parseAsString());
	      JSONArray results = (JSONArray)response.get("result");
	      re = JsonPath.read(results.iterator().next(),"$.name").toString();
//	      for (Object result : results) {
//	        System.out.println(JsonPath.read(result,"$.name").toString());
//	      }
	    } catch (Exception ex) {
	    	re = null;
//	      ex.printStackTrace();
	    }
	  if (re == null || re.length() == 0) {
		  return null;
	  }
	  return re;
  }
  public String long2mid(Long num) {
	  String mid = "";
	  while (num > 0) {
		  int rem = (int) (num % 100);
		  num = num / 100;
		  mid += (char)rem;
	  }
	  return "/m/" + mid.toLowerCase();
  }
  
  public static void main(String[] args) {
	  Convertion c = new Convertion();
//	  System.out.println(c.long2mid(68667078826848));
	  System.out.println(c.mid2Readable(c.long2mid(67518482884948L)));
  }
}