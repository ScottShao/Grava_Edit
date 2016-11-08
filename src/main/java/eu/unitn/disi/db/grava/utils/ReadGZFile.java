package eu.unitn.disi.db.grava.utils;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

/**
 * 
 */

/**
 * @author Zhaoyang
 *
 */
public class ReadGZFile {

	/**
	 * @throws IOException 
	 * 
	 */
	public ReadGZFile(String fileName) throws IOException {
		process(fileName);
	}
	
	private void process(String fileName) throws IOException {
	    // Since there are 4 constructor calls here, I wrote them out in full.
	    // In real life you would probably nest these constructor calls.
	    FileInputStream fin = new FileInputStream(fileName);
	    GZIPInputStream gzis = new GZIPInputStream(fin);
	    InputStreamReader xover = new InputStreamReader(gzis);
	    BufferedReader is = new BufferedReader(xover);
	    BufferedWriter bw = new BufferedWriter(new FileWriter("entities.txt", true));
	    String line;
	    // Now read lines of text: the BufferedReader puts them in lines,
	    // the InputStreamReader does Unicode conversion, and the
	    // GZipInputStream "gunzip"s the data from the FileInputStream.
	    try {
	    	int count = 0;
			while ((line = is.readLine()) != null) {
				if (count > 100) break;
				
				String[] words = line.split("\\t");
				if (words.length < 3) continue;
				System.out.println(line);
				System.out.println("first " + words[0]);
				System.out.println("second " + words[1]);
				System.out.println("third " + words[2]);
				
//				int zero = 0;
//				while (zero < words.length && words[zero].length() == 0) zero++;
//				int first = zero + 1;
//				while (first < words.length && words[first].length() == 0) first++;
//				int second = first + 1;
//				while (second < words.length && words[second].length() == 0) second++;
//				if (second >= words.length) continue;
//				if (words[zero].contains("m.") && words[first].contains("name") && words[second].contains("@en")) {
//					String[] a = words[zero].split("/");
//					bw.write(a[a.length - 1] + " " + words[second].split("\"")[1]);
//					bw.newLine();
//				}
//				if (count % 100000 == 0) System.out.println("Process " + count + " lines");
				count++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			bw.flush();
			if (is != null ) is.close();
			if (bw != null) {
				bw.flush();
				bw.close();
			}
		}
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
//		String a = "masdas";
//		System.out.println(a.contains("m."));
		ReadGZFile rf = new ReadGZFile("freebase.gz");
//		String a = "<http://rdf.freebase.com/ns/american_football.football_player.footballdb_id>    <http://www.w3.org/2000/01/rdf-schema#label>    \"footballdb ID\"@en      .";
//		System.out.println(a.split(" ")[2]);
	}

}
