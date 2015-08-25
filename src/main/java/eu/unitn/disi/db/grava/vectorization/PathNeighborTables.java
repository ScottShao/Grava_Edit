package eu.unitn.disi.db.grava.vectorization;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import eu.unitn.disi.db.grava.exceptions.DataException;
import eu.unitn.disi.db.grava.graphs.PathNeighbor;

public class PathNeighborTables {
	protected Map<Long, Map<PathNeighbor, Integer>[]> levelTables;  
    protected int k;
    
	public PathNeighborTables() {
	}
	
	public PathNeighborTables(int k){
		this.k = k;
		levelTables = new HashMap<>(); 
	}
	

    public boolean addNodeLevelTable(Map<PathNeighbor,Integer> levelNodeTable, long node, short level){
    	Map<PathNeighbor, Integer>[] nodeTable = levelTables.get(node); 
        if (nodeTable == null) {
            nodeTable = new Map[k];
        }
        nodeTable[level] = levelNodeTable;
        return levelTables.put(node, nodeTable) != null;
    }
    
    public boolean addNodeTable(Map<PathNeighbor,Integer>[] nodeTable, Long node) {
        boolean value = true;
        for (short i = 0; i < nodeTable.length; i++) {
            value = addNodeLevelTable(nodeTable[i], node, i) && value;
        }
        return value;
    }
    
    public boolean serialize() throws DataException{
    	return false;
    } 
    
    public Map<PathNeighbor, Integer>[] getNodeMap(long node){
    	return levelTables.get(node);
    }
    
    public Set<Long> getNodes() {
        return levelTables.keySet();
    }
    
    public void merge(PathNeighborTables tables) {
        Set<Long> nodes = tables.getNodes();
        
        for (Long node : nodes) {
            addNodeTable(tables.getNodeMap(node), node);
        }
    }
    
    @Override
    public String toString(){
    	StringBuilder sb = new StringBuilder();
        Map<PathNeighbor, Integer>[] tables;
        Map<PathNeighbor, Integer> levelTable;
        for (Long l : levelTables.keySet()) {
            sb.append("Node: ").append(l).append("\n");
            tables = levelTables.get(l);
            for (int i = 0; i < tables.length; i++) {
                sb.append("[").append(i+1).append("] {");
                levelTable = tables[i];
                for (PathNeighbor pn : levelTable.keySet()) {
                    sb.append("(").append(pn).append(",").append(levelTable.get(pn)).append(")");
                }
                sb.append("}\n");
            }
        }
        return sb.toString();
    }
}
