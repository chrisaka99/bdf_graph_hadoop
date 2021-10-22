package org.graph.tp;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class GraphNodeWritable implements Writable {
    public String couleur = "";
    public int profondeur = 0;
    public String[] voisins = new String[0];
    //public ArrayList<String> graphNode = new ArrayList<String>();
    //public static final String GREY="GRIS";

    public GraphNodeWritable(){}

    public GraphNodeWritable(String data) {
        String[] parts= data.split("\\|");
        System.out.println("DONNE SPLIT :"+ parts[0] +" "+ parts[1]+" "+parts[2]);
        /*if(parts.length!=3) // Invalide.
            return; */
        this.voisins = parts[0].split(",");
        this.couleur = parts[1];
        this.profondeur= -1;
       try {
            this.profondeur = Integer.parseInt(parts[2].trim());
        }
        catch(Exception e) {
            this.profondeur = -2;
        }
    }

    public String get_serialized() {
        return String.join(",", this.voisins) + "|" + this.couleur + "|" + this.profondeur;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(get_serialized().getBytes());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String data = in.readLine();
        String[] parts= data.split("\\|");
        /*if(parts.length!=3) // Invalide.
            return; */
        voisins = parts[0].split(",");
        couleur = parts[1];
        profondeur=-1;
        try {
            profondeur = Integer.parseInt(parts[2].trim());
        } catch(Exception e) {
            profondeur = -2;
        }
    }
}
