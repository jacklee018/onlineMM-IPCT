package MapMatchBaseline;

import MapMatchBaseline.others.RoadsMap;
import com.esri.core.geometry.Point;

import java.io.Serializable;
import java.util.Set;

public class MapMatcher implements Serializable {

    public final RoadsMap map;
    public double radius;
    boolean isMapConstruct = false;

//    public MapMatcher(String mapPath, int maxRadius) {
//        this.map = RoadMap.Load(new BfmapReader(mapPath));
//        this.radius = maxRadius;
//    }

    public MapMatcher(String mapPathp, int maxRadius) {
        this.map = new RoadsMap();
        this.radius = maxRadius;
    }

    public Point mapMatch(double x, double y) {
//        long time1 = System.currentTimeMillis();
        if (!isMapConstruct) {
            synchronized (this) {
                if (!isMapConstruct) {
                    map.construct();
                    isMapConstruct = true;
                }
            }
        }
        Point samplePoint = new Point(x, y);
        Set<Point> points_ = map.radius(samplePoint, radius);
//        long time2 = System.currentTimeMillis();
//        System.out.println("获取候选道路耗时：" + (time2 - time1));
        double minDistance = Double.MAX_VALUE;
        Point nearestPoint = null;
        for (Point point : points_) {
            double dz = gpsDistance(samplePoint.getX(), samplePoint.getY(), point.getX(), point.getY());
            if (minDistance > dz) {
                minDistance = dz;
                nearestPoint = point;
            }
        }
//        long time3 = System.currentTimeMillis();
//        System.out.println("计算耗时：" + (time3 - time2));
        return nearestPoint;
    }

    public double gpsDistance(double lon1, double lat1, double lon2, double lat2) {

        final int R = 6371; // Radius of the earth

        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c * 1000; // convert to meters


        distance = Math.pow(distance, 2);

        return Math.sqrt(distance);
    }
}
