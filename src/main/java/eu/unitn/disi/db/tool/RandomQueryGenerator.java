package eu.unitn.disi.db.tool;

import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class RandomQueryGenerator {

    private static final Random RANDOM = new Random();

    private static void writeToFile(Set<String> query, String node, int maxFreq, int crtFreq) {
        try {
            String fileName = "queryFolder/10000nodes/random-freq-" + maxFreq + "/E8" + node + "_" + crtFreq + ".txt";
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
        List<Edge> list = new ArrayList<>(edges);
        return list.get(RANDOM.nextInt(list.size()));
    }

    public void generateQuery(Multigraph G) {
        int maxEdgeNum = 8;
        G.vertexSet();
        List<Long> nodes = new ArrayList<>(G.vertexSet());
        Set<String> query = new HashSet<>();
        int currentFreq = 0;
        int maxFreq = 1500;
        Long startingNode = nodes.get(RANDOM.nextInt(nodes.size()));
        int edgeNum = 0;
        Set<Long> visited = new HashSet<>();
        Set<Edge> edgesNotWanted = new HashSet<>();
        Long currentNode = startingNode;
        visited.add(currentNode);
        while (edgeNum < maxEdgeNum) {
            Set<Edge> candidates = new HashSet<>();
            if (RANDOM.nextBoolean()) {
                for (Edge edge : G.incomingEdgesOf(currentNode)) {
                    if (!visited.contains(edge.getSource()) && !edgesNotWanted.contains(edge)) {
                        candidates.add(edge);
                    }
                }
            } else {
                for (Edge edge : G.outgoingEdgesOf(currentNode)) {
                    if (!visited.contains(edge.getDestination()) && !edgesNotWanted.contains(edge)) {
                        candidates.add(edge);
                    }
                }
            }
            if (candidates.isEmpty()) {
                break;
            }
            while (!candidates.isEmpty()) {
                Edge edge = getRandomEdge(candidates);
                int freq = G.getLabelFreq().get(edge.getLabel()).getFrequency();
                if (currentFreq + freq <= maxFreq) {
                    query.add(edgeOutput(edge));
                    edgeNum++;
                    currentFreq += freq;
                    Long nextNode = edge.getSource().equals(currentNode) ? edge.getDestination() : edge.getSource();
                    currentNode = RANDOM.nextBoolean() ? nextNode : currentNode;
                    break;
                } else {
                    candidates.remove(edge);
                    edgesNotWanted.add(edge);
                }
            }
        }

        if (query.size() >= maxEdgeNum && currentFreq >= maxFreq - 10) {
            writeToFile(query, String.valueOf(startingNode), maxFreq, currentFreq);
        }
    }
}
