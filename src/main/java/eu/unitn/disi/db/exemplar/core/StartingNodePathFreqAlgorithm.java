package eu.unitn.disi.db.exemplar.core;

import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.utils.BloomFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class StartingNodePathFreqAlgorithm implements StartingNodeAlgorithm {

    private final Map<String, Integer> pathFreqMap;
    private final int totalNumberOfPath;
    private final int pathLength;


    public StartingNodePathFreqAlgorithm(final Map<String, Integer> pathFreqMap,
                                         final int totalNumberOfPath,
                                         final int pathLength) {
        this.pathFreqMap = pathFreqMap;
        this.totalNumberOfPath = totalNumberOfPath;
        this.pathLength = pathLength;
    }

    @Override
    public Long getStartingNode(final Multigraph query) {
        double min = 1;
        Long startingNode = null;
        for (Long node : query.vertexSet()) {
            Map<String, Integer> countMap = new HashMap<>();
            dfs(query, node, new HashSet<>(), new HashSet<>(), countMap, new StringBuilder(), 0, new ArrayList<>());
            double selectivity = getSelectivity(countMap);
            if (selectivity < min) {
                min = selectivity;
                startingNode = node;
            }
        }
        return startingNode;
    }

    @Override
    public List<Long> getStartingNodes(final Multigraph query) {
        double min = 1;
        Map<Long, Double> selectivityMap = new HashMap<>();
        for (Long node : query.vertexSet()) {
            Map<String, Integer> countMap = new HashMap<>();
            dfs(query, node, new HashSet<>(), new HashSet<>(), countMap, new StringBuilder(), 0, new ArrayList<>());
            double selectivity = getSelectivity(countMap);
            selectivityMap.put(node, selectivity);
        }
        List<Long> results = selectivityMap.entrySet().stream().sorted(Map.Entry.comparingByValue()).map(Entry::getKey).collect(
                Collectors.toList());
        return results;
    }

    private double getSelectivity(final Map<String, Integer> countMap) {
        double sel = 1;
        for (Entry<String, Integer> entry : countMap.entrySet()) {
            int freq = pathFreqMap.get(entry.getKey());
            sel *= Math.pow(freq / (double)totalNumberOfPath, entry.getValue());
        }
        return sel;
    }

    private void dfs(final Multigraph query, final Long currentNode,
                    final Set<Edge> visited, final Set<Long> visitedNodes,
                    final Map<String, Integer> countMap, final StringBuilder sb,
                    final int depth, final List<String> labels) {

        if (depth >= pathLength) {
            String path = visitedNodes.contains(currentNode) ? sb.toString() + "L" : sb.toString();
            addPath(countMap, path);
            return;
        }
        int length = sb.length();
        visitedNodes.add(currentNode);
        int size = labels.size();
        for (Edge e : query.outgoingEdgesOf(currentNode)) {
            Long nextNode = e.getDestination().equals(currentNode) ? e.getSource() : e.getDestination();
            if (!visited.contains(e) && !nextNode.equals(currentNode)) {
                Long temp = e.getLabel();
                sb.append(temp);
                labels.add(String.valueOf(temp));
                visited.add(e);
                dfs(query, nextNode, visited, visitedNodes, countMap, sb, depth + 1, labels);
                visited.remove(e);
                sb.setLength(length);
                labels.remove(size);
            }
        }

        for (Edge e : query.incomingEdgesOf(currentNode)) {
            Long nextNode = e.getDestination().equals(currentNode) ? e.getSource() : e.getDestination();
            if (!visited.contains(e) && !nextNode.equals(currentNode)) {
                String temp = "-" + e.getLabel();
                sb.append(temp);
                labels.add(temp);
                visited.add(e);
                dfs(query, nextNode, visited, visitedNodes,countMap, sb, depth + 1, labels);
                visited.remove(e);
                labels.remove(size);
                sb.setLength(length);
            }
        }
        visitedNodes.remove(currentNode);
        if (labels.size() == 1) {
            addPath(countMap, String.valueOf(labels.get(0)));
        }
    }

    private void addPath(Map<String, Integer> countMap, String path) {
        Integer count = countMap.getOrDefault(path, 0);
        count++;
        countMap.put(path, count);
    }
}
