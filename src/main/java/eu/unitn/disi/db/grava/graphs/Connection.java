/**
 * 
 */
package eu.unitn.disi.db.grava.graphs;

/**
 * @author Zhaoyang
 *
 */
public class Connection {

	private long first;
	private long second;
	private int freq;
	private int firstFreq;
	
	public Connection() {
	}
	
	public Connection(long first, long second) {
		this.first = first;
		this.second = second;
		freq = 0;
		this.firstFreq = 0;
	}
	
	public int getFirstFreq() {
		return firstFreq;
	}

	public void setFirstFreq(int firstFreq) {
		this.firstFreq = firstFreq;
	}

	public void increment() {
		freq++;
	}
	
	public int getFreq() {
		return freq;
	}

	public void setFreq(int freq) {
		this.freq = freq;
	}

	public boolean equals(Object o) {
		Connection c = (Connection)o;
		return c.getFirst() == this.first && c.getSecond() == this.second;
	}
	
	public int hashCode() {
		return new Long(first + second).hashCode();
	}

	public long getFirst() {
		return first;
	}

	public void setFirst(long first) {
		this.first = first;
	}

	public long getSecond() {
		return second;
	}

	public void setSecond(long second) {
		this.second = second;
	}
	
	

}
