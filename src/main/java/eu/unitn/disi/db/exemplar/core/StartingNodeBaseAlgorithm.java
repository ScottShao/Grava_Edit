package eu.unitn.disi.db.exemplar.core;

import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.LabelContainer;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class StartingNodeBaseAlgorithm implements StartingNodeAlgorithm {

    private HashMap<Long, LabelContainer> labelFreqs;

    public StartingNodeBaseAlgorithm(HashMap<Long, LabelContainer> labelFreqs) {
        this.labelFreqs = labelFreqs;
    }

    @Override
    public Long getStartingNode(final Multigraph query) {
        return getStartingNodes(query).get(0);
    }

    @Override
    public List<Long> getStartingNodes(final Multigraph query) {
        Map<Long, Integer> freqMap = new HashMap<>();
        Collection<Long> nodes = query.vertexSet();
        Set<Long> edgeLabels = new HashSet<>();
        int maxFreq = 0, tempFreq = 0;
        Long goodNode = null;

        for (Edge l : query.edgeSet()) {
            edgeLabels.add(l.getLabel());
        }

        Long bestLabel = -1L;

        bestLabel = this.findLessFrequentLabel(edgeLabels);

        if (bestLabel == null || bestLabel == -1L) {
            throw new RuntimeException("Best Label not found when looking for a root node!");
        }

        Collection<Edge> edgesIn, edgesOut;

        for (Long concept : nodes) {
            tempFreq = query.inDegreeOf(concept) + query.outDegreeOf(concept);
            for (Edge e : query.incomingEdgesOf(concept)) {
                Long nextNode = e.getDestination().equals(concept) ? e.getSource() : e.getDestination();
                tempFreq += query.inDegreeOf(nextNode) + query.outDegreeOf(nextNode);
            }
            for (Edge e : query.outgoingEdgesOf(concept)) {
                Long nextNode = e.getDestination().equals(concept) ? e.getSource() : e.getDestination();
                tempFreq += query.inDegreeOf(nextNode) + query.outDegreeOf(nextNode);
            }
            freqMap.put(concept, tempFreq);
        }
        List<Long> results = freqMap.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .map(Entry::getKey)
                .collect(Collectors.toList());
        Collections.reverse(results);
        return results;
    }

    private Long findLessFrequentLabel(final Set<Long> edgeLabels) {
        Long candidate = null;
        int freq = Integer.MAX_VALUE;
        for (Long label : edgeLabels) {
            if (label.equals(0L)) {
                continue;
            }
            if (labelFreqs.get(label).getFrequency() < freq) {
                candidate = label;
            }
        }
        return candidate;
    }
}
