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
	    BufferedWriter bw = new BufferedWriter(new FileWriter("entities.txt"));
	    String line;
	    // Now read lines of text: the BufferedReader puts them in lines,
	    // the InputStreamReader does Unicode conversion, and the
	    // GZipInputStream "gunzip"s the data from the FileInputStream.
	    try {
			while ((line = is.readLine()) != null) {
				String[] words = line.split(" ");
				if (words[0].contains("m.") && words[1].contains("object.name") && words[2].contains("@en")) {
					String[] a = words[0].split("/");
					bw.write(a[a.length - 1] + " " + words[2].split("\"")[1]);
				}
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
		String test = "\"object\"@en";
		System.out.println(test.split("\"")[1]);
	}

}
