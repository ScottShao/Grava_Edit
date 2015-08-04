package eu.unitn.disi.db.grava.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map.Entry;

import eu.unitn.disi.db.grava.graphs.LabelContainer;

public class LabelsUltility {
	public static long[] droppingLabels(HashMap<Long, LabelContainer> labelFreq, int droppedLabelsCount){
		long[] droppedLabels = new long[droppedLabelsCount];
		int[] maxFreq = new int[droppedLabelsCount];
		int freq = -1;
		LabelContainer lc = null;
		long l;
		boolean flag = false;
		for(Entry<Long, LabelContainer> temp : labelFreq.entrySet()){
			lc = temp.getValue();
			freq = lc.getFrequency();
			l = lc.getLabelID();
			flag = false;
			//StdOut.println("count" + droppedLabelsCount);
			for (int i = 0; i < droppedLabelsCount; i++){
				if(l == droppedLabels[i]){
					flag = true;
					break;
				}
			}
			if(flag){
				continue;
			}
			if(freq > maxFreq[droppedLabelsCount-1]){
				//droppedLabels[droppedLabelsCount-1] = lc.getLabelID();
				for(int i = 0; i < droppedLabelsCount; i++){
					if(freq > maxFreq[i]){
						for(int j = droppedLabelsCount - 1; j > i; j--){
							maxFreq[j] = maxFreq[j-1];
							droppedLabels[j] = droppedLabels[j-1];
						}
						maxFreq[i] = freq;
						droppedLabels[i] = l;
						break;
					}else{
						continue;
					}
				}
			}
		}
		return droppedLabels;
	}
	
	public static HashSet<Long> mergeNodes(HashSet<Long> firstSet, HashSet<Long> secSet){
		if(firstSet == null){
			return secSet;
		}
		if(secSet == null){
			return firstSet;
		}
		for(Long temp : secSet){
			firstSet.add(temp);
		}
		return firstSet;
	}

}
