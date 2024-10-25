package common;

import org.apache.commons.math3.distribution.NormalDistribution;

public class RegionUtil {

    static Region region_459 = new Region(new GpsPoint(113.923486, 22.561342), new GpsPoint(113.956572, 22.586826));

    static Region.Area area1 = region_459.getArea(22, 1, 34, 29, new NormalDistribution(0.4051389, 6.967218829), 1.8907485);
    static Region.Area area2 = region_459.getArea(10, 6, 18, 7, new NormalDistribution(12.869664, 20.3106212), 0.8490343, new NormalDistribution(35.56625702, 2.900092), 0.4101685);
    static Region.Area area3 = region_459.getArea(15, 25, 20, 29, new NormalDistribution(-6.5111784, 26.5726179), 1.989694, new NormalDistribution(35.5550069, 2.6531335), 0.238818);

    static Region.Area[] areas_459 = { area1, area2, area3 };

    public static Region.Area getArea(double lon, double lat) {
        if (area1.isInArea(lon, lat)) return area1;
        else if (area2.isInArea(lon, lat)) return area2;
        else if (area3.isInArea(lon, lat)) return area3;
        return area1;
    }

}
