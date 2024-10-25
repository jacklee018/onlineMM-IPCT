package bfGpsDataMatch;


import com.esri.core.geometry.Point;

public class HmmPoint {

    public String id;
    public Point point;
    public double filterProb = 0d;
    public int preIndex = -1;
}
