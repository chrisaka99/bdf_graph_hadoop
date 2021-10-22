package org.graph.tp;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;

public class GraphReduce extends Reducer<Text, GraphNodeWritable, Text, GraphNodeWritable>
{
    public void reduce(Text key, Iterable<GraphNodeWritable> values, Context context) throws IOException,
            InterruptedException
    {

        int new_depth=-1;
        String[] new_neighbours = {};
        String new_color="";
        Iterator<GraphNodeWritable> i=values.iterator();
        while(i.hasNext())
        {
            GraphNodeWritable node=i.next();

            if(node.profondeur>new_depth)
                new_depth=node.profondeur;
            if(node.voisins.length>new_neighbours.length)
                new_neighbours=node.voisins;
            if((new_color.equals("")) || ((new_color.equals("BLANC") && (node.couleur.equals("GRIS") || node.couleur.equals("NOIR"))) || (new_color.equals("GRIS") && (node.couleur.equals("NOIR")))))
            {
                new_color=node.couleur;
            }

        }
        if(!new_color.equals("NOIR"))
            context.getCounter(Graph.GRAPH_COUNTERS.NB_NODES_UNFINISHED).increment(1);

        context.write(key, new GraphNodeWritable(String.join(",", new_neighbours)+"|"+new_color+"|"+new_depth));
    }
}
