package common;

import org.apache.commons.math3.distribution.NormalDistribution;
import utils.CommonUtils;

import java.io.Serializable;

/**
 * Rectangle Area
 */
public class Region implements Serializable {

    final int gridSize = 100;

    /**
     * left below
     */
    final GpsPoint lb_point;

    /**
     * right upper
     */
    final GpsPoint ru_point;

    public final int xLength; // x range
    public final int yLength; // y range


    public Region(GpsPoint lb_point, GpsPoint ru_point) {
        this.lb_point = lb_point;
        this.ru_point = ru_point;
        double length = CommonUtils.gpsDistance(lb_point.longitude, lb_point.latitude, ru_point.longitude, lb_point.latitude);
        double width = CommonUtils.gpsDistance(lb_point.longitude, lb_point.latitude, lb_point.longitude, ru_point.latitude);
        xLength = (int) (length / gridSize) + (length % gridSize == 0 ? 0 : 1);
        yLength = (int) (width / gridSize) + (width % gridSize == 0 ? 0 : 1);
    }

    public int getOrderInRegion(double longitude, double latitude) {
        if (!inRegion(longitude, latitude)) return -1;
        double length = CommonUtils.gpsDistance(longitude, latitude, lb_point.longitude, latitude);
        double width = CommonUtils.gpsDistance(longitude, latitude, longitude, lb_point.latitude);
        int lenNum = (int) (length / gridSize) + (length % gridSize == 0 ? 0 : 1);if (lenNum==0) lenNum++;
        int widNum = (int) (width / gridSize) + (width % gridSize == 0 ? 0 : 1);if (widNum==0) widNum++;
        return (widNum - 1) * xLength + lenNum;
    }

    public int[] getCoordinateInRegion(double longitude, double latitude) {
        if (!inRegion(longitude, latitude)) return new int[]{-1,-1};
        double length = CommonUtils.gpsDistance(longitude, latitude, lb_point.longitude, latitude);
        double width = CommonUtils.gpsDistance(longitude, latitude, longitude, lb_point.latitude);
        int x = (int) (length / gridSize) + (length % gridSize == 0 ? 0 : 1);if (x==0) x++;
        int y = (int) (width / gridSize) + (width % gridSize == 0 ? 0 : 1);if (y==0) y++;
        return new int[]{x,y};
    }

    public boolean inRegion(GpsPoint p) {
        return p.longitude >= lb_point.longitude && p.longitude <= ru_point.longitude
                && p.latitude >= lb_point.latitude && p.latitude <= ru_point.latitude;
    }

    public boolean inRegion(double longitude, double latitude) {
        return longitude >= lb_point.longitude && longitude <= ru_point.longitude
                && latitude >= lb_point.latitude && latitude <= ru_point.latitude;
    }

    public Area getArea(int x1, int y1, int x2, int y2, NormalDistribution dist1, double a1) {
        return new Area(x1, y1, x2, y2, dist1, a1);
    }

    public Area getArea(int x1, int y1, int x2, int y2, NormalDistribution dist1, double a1, NormalDistribution dist2, double a2) {
        return new Area(x1, y1, x2, y2, dist1, a1, dist2, a2);
    }


    /**
     * Rectangle with the Region
     */
    public class Area implements Serializable {
        int x1, y1; // left below coordinate
        int x2, y2; // right upper coordinate
        NormalDistribution dist1, dist2;
        double a1, a2;
        boolean isDoubleDist = false;
        public int radius = 50;

        public Area(int x1, int y1, int x2, int y2, NormalDistribution dist1, double a1) {
            this.x1 = x1;
            this.y1 = y1;
            this.x2 = x2;
            this.y2 = y2;
            this.dist1 = dist1;
            this.a1 = a1;
            radius = (int)dist1.inverseCumulativeProbability(0.9999);
        }

        public Area(int x1, int y1, int x2, int y2, NormalDistribution dist1, double a1, NormalDistribution dist2, double a2) {
            this.x1 = x1;
            this.y1 = y1;
            this.x2 = x2;
            this.y2 = y2;
            this.dist1 = dist1;this.a1 = a1;
            this.dist2 = dist2;this.a2 = a2;
            isDoubleDist = true;
        }

        public boolean isInArea(double longitude, double latitude) {
            int[] c = getCoordinateInRegion(longitude, latitude);
            return c[0] >= x1 && c[0] <= x2 && c[1] >= y1 && c[1] <= y2;
        }

        public double gaussianPDF(double d) {
            if (!isDoubleDist) {
                return a1 * dist1.density(d);
            }
            double pdf1 = a1 * dist1.density(d);
            double pdf2 = a2 * dist2.density(d);

            return pdf1 + pdf2;
        }


    }
}
