package eu.unitn.disi.db.tool;

import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class FanShapeQueryGenerator {
    private static final Random RANDOM = new Random();

    public void generateQuery(Multigraph G) {
        int maxEdgeNum = 8;
        int current = 0;
        G.vertexSet();
        List<Long> nodes = new ArrayList<>(G.vertexSet());
        Set<String> query = new HashSet<>();
        Set<Long> labels = new HashSet<>();
        int currentFreq = 0;
        int maxFreq = 500;
        int degree = 4;
        LinkedList<Long> queue = new LinkedList<>();
        Long startingNode = nodes.get(RANDOM.nextInt(nodes.size()));
        queue.add(startingNode);
        int edgeNum = 0;
        Set<Long> visited = new HashSet<>();
        while (!queue.isEmpty()) {
            Long currentNode = queue.pollFirst();
            visited.add(currentNode);
            Set<Edge> candidates = new HashSet<>();
            for (Edge edge : G.incomingEdgesOf(currentNode)) {
                if (!visited.contains(edge.getSource())) {
                    candidates.add(edge);
                }
            }
            for (Edge edge : G.outgoingEdgesOf(currentNode)) {
                if (!visited.contains(edge.getDestination())) {
                    candidates.add(edge);
                }
            }
            if (candidates.size() < degree) {
                continue;
            }
            int count = 0;
            while (count < degree) {
                Edge edge = getRandomEdge(candidates);
                Long nextNode = edge.getSource().equals(currentNode) ? edge.getDestination() : edge.getSource();
                int freq = G.getLabelFreq().get(edge.getLabel()).getFrequency();
                if (!visited.contains(nextNode) && currentFreq + freq <= maxFreq) {
                    query.add(edgeOutput(edge));
                    queue.add(nextNode);
                    count ++;
                    edgeNum++;
                    currentFreq += freq;
                    visited.add(nextNode);
                }
                candidates.remove(edge);

                if (edgeNum >= maxEdgeNum || candidates.isEmpty()) {
                    break;
                }
            }
            if (edgeNum >= maxEdgeNum) {
                break;
            }
        }
        if (edgeNum >= maxEdgeNum) {
            writeToFile(query, startingNode + "_freq_" + currentFreq);
        }
    }

    private static void writeToFile(Set<String> query, String node) {
        try {
            String fileName = "queryFolder/10000nodes/freq-500/E8" + node + ".txt";
            final BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
            for (String str : query) {
                writer.write(str);
                writer.newLine();
            }
            writer.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    private static String edgeOutput(Edge edge) {
        return edge.getSource() + " " + edge.getDestination() + " " + edge.getLabel();
    }

    private static Edge getRandomEdge(Collection<Edge> edges) {
        if (edges.size() == 0) {
            return null;
        }
        List<Edge> list = new ArrayList<>(edges);
        return list.get(RANDOM.nextInt(list.size()));
    }
}
