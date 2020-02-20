package eu.unitn.disi.db.tool;

import eu.unitn.disi.db.exemplar.core.IsomorphicQuery;
import eu.unitn.disi.db.exemplar.core.RelatedQuery;
import eu.unitn.disi.db.grava.graphs.Edge;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

public class AnswerManagement {

    public static void printIsomorphicAnswer(Collection<IsomorphicQuery> queries) {
        if (queries == null) {
            System.out.println("Null answers");
            return;
        }
        System.out.println("printing answers");
        int num = 1;
//		System.out.println(queries.get(0));
        System.out.println("========================================");
        for (RelatedQuery rq : queries) {
            Map<Edge, Edge> mappedEdges = rq.getMappedEdges();
            Map<Long, Long> answerNodes = new HashMap<>();
            boolean right = true;
            for (Entry<Edge, Edge> en : mappedEdges.entrySet()) {
                Edge queryEdge = en.getKey();
                Edge graphEdge = en.getValue();
                if (queryEdge.getSource() < 0) {
                    if (!queryEdge.getSource().equals(-graphEdge.getSource())) {
                        right = false;
                        break;
                    }
                } else {
                    answerNodes.put(queryEdge.getSource(), graphEdge.getSource());
                }
                if (queryEdge.getDestination() < 0) {
                    if (!queryEdge.getDestination().equals(-graphEdge.getDestination())) {
                        right = false;
                        break;
                    }
                } else {
                    answerNodes.put(queryEdge.getDestination(), graphEdge.getDestination());
                }
            }
            if (right) {
                System.out.println("Printing answer " + num++);

                for (Edge e : mappedEdges.values()) {
//					System.out.println(this.id2Entity.get(e.getSource()).trim() + " " + this.id2Predicate.get(e.getLabel()).trim() + " " + this.id2Entity.get(e.getDestination()).trim());
                    System.out.println(e.getSource() + " " + e.getDestination() + " " + e.getLabel());
                }
                System.out.println("========================================");
            }
//			answers.add(this.id2Entity.get(eq.getNodesMapping().get(this.nodeToSearch).getNodeID()).trim());
        }
    }

    public static void printAnswer(Collection<RelatedQuery> queries) {
        System.out.println("printing answers");
        int num = 1;
        System.out.println("========================================");
        PriorityQueue<Edge> edges = new PriorityQueue<>(new Comparator<Edge>() {
            @Override
            public int compare(final Edge o1, final Edge o2) {
                return o1.toString().compareTo(o2.toString());
            }
        });
        List<RelatedQuery> sortedQueries = new ArrayList<>(queries);
        Collections.sort(sortedQueries, new Comparator<RelatedQuery>() {
            @Override
            public int compare(final RelatedQuery o1, final RelatedQuery o2) {
                return o1.toString().compareTo(o2.toString());
            }
        });
        for (RelatedQuery rq : sortedQueries) {
            Map<Edge, Edge> mappedEdges = rq.getMappedEdges();
            Map<Long, Long> answerNodes = new HashMap<>();
            boolean right = true;
            for (Entry<Edge, Edge> en : mappedEdges.entrySet()) {
                Edge queryEdge = en.getKey();
                Edge graphEdge = en.getValue();
                if (queryEdge.getSource() < 0) {
                    if (!queryEdge.getSource().equals(-graphEdge.getSource())) {
                        right = false;
                        break;
                    }
                } else {
                    if (graphEdge == null) {
                        System.out.println("null graph edge with query edge:" + queryEdge);
                    }
                    answerNodes.put(queryEdge.getSource(), graphEdge.getSource());
                }
                if (queryEdge.getDestination() < 0) {
                    if (!queryEdge.getDestination().equals(-graphEdge.getDestination())) {
                        right = false;
                        break;
                    }
                } else {
                    answerNodes.put(queryEdge.getDestination(), graphEdge.getDestination());
                }
            }
            if (right) {
                System.out.println("Printing answer " + num++);

//                edges.addAll(mappedEdges.values());
//               while (!edges.isEmpty()){
//                   Edge e = edges.poll();
//                    System.out.println(e.getSource() + " " + e.getDestination() + " " + e.getLabel());
//                }
                System.out.print(rq.toString());
                System.out.println("========================================");
            }
        }
    }
}
