package eu.unitn.disi.db.tool;

import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
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

public class QueryGenerator {
    private static final Random RANDOM = new Random();
    public static void main(String[] args) throws IOException, ParseException {
        String graphName = "graph/10000nodes";

        Multigraph G = new BigMultigraph(graphName + "-sin.graph", graphName
                + "-sout.graph" );
        for (int i = 0; i < 10000; i ++) {
            generateQuery(G);
        }
    }

    private static void generateQuery(Multigraph G) {
        int edgeNum = 8;
        int current = 0;
        G.vertexSet();
        List<Long> nodes = new ArrayList<>(G.vertexSet());
        Long currentNode = nodes.get(RANDOM.nextInt(nodes.size()));
        Set<String> query = new HashSet<>();
        Set<Long> labels = new HashSet<>();
        int count = 0;
        int currentFreq = 0;
        int maxFreq = 300;
        while (query.size() < edgeNum) {
            count ++;
            if (count > 500) {
                break;
            }
            if (RANDOM.nextBoolean()) {
                Edge incomingEdge = getRandomEdge(G.incomingEdgesOf(currentNode));
                if (incomingEdge == null) {
                    continue;
                }
                if (labels.contains(incomingEdge.getLabel())) {
                    continue;
                }
                currentFreq += G.getLabelFreq().get(incomingEdge.getLabel()).getFrequency();
                if (currentFreq > maxFreq) {
                    continue;
                }
                labels.add(incomingEdge.getLabel());
                query.add(edgeOutput(incomingEdge));
                if (RANDOM.nextBoolean()) {
                    currentNode = incomingEdge.getSource();
                }
            } else {
                Edge outgoingEdge = getRandomEdge(G.outgoingEdgesOf(currentNode));
                if (outgoingEdge == null) {
                    continue;
                }
                if (labels.contains(outgoingEdge.getLabel())) {
                    continue;
                }
                currentFreq += G.getLabelFreq().get(outgoingEdge.getLabel()).getFrequency();
                if (currentFreq > maxFreq) {
                    continue;
                }
                labels.add(outgoingEdge.getLabel());
                query.add(edgeOutput(outgoingEdge));
                if (RANDOM.nextBoolean()) {
                    currentNode = outgoingEdge.getDestination();
                }
            }
        }
        if (count < 500) {
            writeToFile(query, String.valueOf(currentNode));
        }
    }

    private static void writeToFile(Set<String> query, String node) {
        try {
            String fileName = "queryFolder/10000nodes/E8" + node + ".txt";
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
