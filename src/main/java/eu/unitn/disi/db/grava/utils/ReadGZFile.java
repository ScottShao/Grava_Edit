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
//				System.out.println(line);

				if (words[0].contains("m.") && words[1].contains("name") && words[2].contains("@en")) {
					String[] a = words[0].split("/");
					System.out.println(a[a.length - 1].substring(0, a[a.length - 1].length() - 1) + " " + words[2].substring(1, words[2].length() - 4));
					bw.write(a[a.length - 1].substring(0, a[a.length - 1].length() - 1) + " " + words[2].substring(1, words[2].length() - 4));
					bw.newLine();
					count++;
				}
				if (count % 100000 == 0) System.out.println("Process " + count + " lines");
				
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
		String a = "<http://rdf.freebase.com/ns/award.award_winner>";
		System.out.println(a.split("/")[a.split("/").length-1]);
//		ReadGZFile rf = new ReadGZFile("freebase.gz");
//		String a = "<http://rdf.freebase.com/ns/american_football.football_player.footballdb_id>    <http://www.w3.org/2000/01/rdf-schema#label>    \"footballdb ID\"@en      .";
//		System.out.println(a.split(" ")[2]);
	}

}
