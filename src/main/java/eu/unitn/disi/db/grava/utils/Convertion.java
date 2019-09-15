/**
 *
 */
package eu.unitn.disi.db.grava.utils;

/**
 * @author Zhaoyang
 *
 */

import eu.unitn.disi.db.grava.graphs.Edge;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Convertion {

    public static Properties properties;
    public static Map<Long, String> predMap;
    public static Map<String, String> map;

    public Convertion() {
        properties = new Properties();
        predMap = null;
        map = new HashMap<>();
        init();
    }

    public static String convertLongToMid(long decimal) throws NullPointerException, IndexOutOfBoundsException {
        String mid = "";
        String decimalString = decimal + "";

        for (int i = 0; i < decimalString.length(); i += 2) {
            if (decimalString.length() < 5) {
                mid = decimalString;
            } else {
                mid = (char) Integer.parseInt(decimalString.substring(i, i + 2)) + mid;
            }


        }

        return "/m/" + mid.toLowerCase();
    }

    public static Map<String, String> getMap() {
        return map;
    }

    public static void setMap(Map<String, String> map) {
        Convertion.map = map;
    }

    public static void main(String[] args) {
        Convertion c = new Convertion();
        System.out.println("/a/".split("/")[1]);
//	  System.out.println(c.mid2Readable(c.long2mid(51808657866848L)));
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

    public String toReadable(Edge e) {
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
//	  try {
////		properties.load(new FileInputStream("freebase.properties"));
//		predMap = new HashMap<>();
//		BufferedReader br = new BufferedReader(new FileReader("predicatesWithID.txt"));
//		String line = null;
//		while((line = br.readLine()) != null){
//			String[] words = line.split(" ");
//			String[] wws = words[0].split("\\.");
//			predMap.put(Long.valueOf(words[1]), wws[wws.length - 1]);
//		}
//		br.close();
//		loadEntities();
//	} catch (IOException e) {
//		e.printStackTrace();
//	}
    }

    public void loadEntities() {
//		try {
//			BufferedReader br = new BufferedReader(new FileReader("entities.txt"));
//			String line = null;
//			int count = 0;
//			while ((line = br.readLine()) != null) {
//				String[] words = line.split(" ");
//				if (words[0] == null) System.out.println("null");
//
//				String entity = line.substring(words[0].length() + 1);
//				if (entity == null) System.out.println("asd");
//				map.put("/m/" + words[0].substring(2), entity);
//				count++;
////				if (count % 10000 == 0) System.out.println("/m/" + words[0].substring(2));
//			}
//			System.out.println("Entities loaded.");
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//
    }

    public String mid2Readable(String mid) {
        return map.get(mid);
    }

    public String long2mid(Long num) {
        String mid = "";
        while (num > 0) {
            int rem = (int) (num % 100);
            num = num / 100;
            mid += (char) rem;
        }
        return "/m/" + mid.toLowerCase();
    }
}