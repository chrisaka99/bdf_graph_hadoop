package org.graph.tp;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GraphMap extends Mapper<Text, GraphNodeWritable, Text, GraphNodeWritable>
{
    private static final String GREY="GRIS";

    protected void map(Text key, GraphNodeWritable node, Context context) throws IOException, InterruptedException
    {  /*
        String[] parts=node.toString().split("\\|");
        if(parts.length!=3) // Invalide.
            return;
        String[] neighbours=parts[0].split(",");
        String colour=parts[1];
        int depth=-1;
        try {
            depth=Integer.parseInt(parts[2]);
        } catch(Exception e) {
            depth=-1;
        }
        */


        if(node.couleur.equals(GREY))
        {
            for(int i=0; i<node.voisins.length; ++i)
            {
                if(node.voisins[i].equals(""))
                    continue;
                context.write(new Text(node.voisins[i]), new GraphNodeWritable("|"+GREY+"|"+Integer.toString(node.profondeur+1)));
            }
            context.write(key, new GraphNodeWritable(String.join(",", node.voisins)+"|NOIR|"+Integer.toString(node.profondeur)));
        }
        else
            context.write(key, node);

    }
}
