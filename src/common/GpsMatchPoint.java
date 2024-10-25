package common;

import com.bmwcarit.barefoot.matcher.MatcherCandidate;
import com.bmwcarit.barefoot.matcher.MatcherSample;
import com.bmwcarit.barefoot.roadmap.Road;
import com.bmwcarit.barefoot.roadmap.RoadPoint;
import com.bmwcarit.barefoot.util.Tuple;
import com.esri.core.geometry.Point;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class GpsMatchPoint implements Serializable {

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

    public double getOriginalX() {
        return originalX;
    }

    public void setOriginalX(double originalX) {
        this.originalX = originalX;
    }

    public double getOriginalY() {
        return originalY;
    }

    public void setOriginalY(double originalY) {
        this.originalY = originalY;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getRoad() {
        return road;
    }

    public void setRoad(long road) {
        this.road = road;
    }

    // original gps coordinates
    double originalX;
    double originalY;

    long time;

    // start matching points
    double x;
    double y;

    // start matching roads
    long road;

    public List<Long> getPathIds() {
        return pathIds;
    }

    public void setPathIds(List<Long> pathIds) {
        this.pathIds = pathIds;
    }

    // Road path from the previous point to the current point
    List<Long> pathIds = new ArrayList<>();

    public GpsMatchPoint(RoadPoint roadPoint) {
        this.x = roadPoint.geometry().getX();
        this.y = roadPoint.geometry().getY();
        this.road = roadPoint.edge().base().refid();
    }

    public GpsMatchPoint(Point originalPoint, long time, RoadPoint matchPoint) {
        this.originalX = originalPoint.getX();
        this.originalY = originalPoint.getY();
        this.time = time;
        this.x = matchPoint.geometry().getX();
        this.y = matchPoint.geometry().getY();
        this.road = matchPoint.edge().base().refid();
    }

    public GpsMatchPoint(Tuple<MatcherCandidate, MatcherSample> t) {
        RoadPoint matchPoint = t.one().point();
        Point originalPoint = t.two().point();
        this.originalX = originalPoint.getX();
        this.originalY = originalPoint.getY();
        this.time = t.two().time();
        this.x = matchPoint.geometry().getX();
        this.y = matchPoint.geometry().getY();
        this.road = matchPoint.edge().base().refid();

        if (t.one().transition() != null)
            this.pathIds = t.one().transition()
                .route()
                .path()
                .stream().map(r -> r.base().refid()).collect(Collectors.toList());
        else {
            //如果是起始点
            this.pathIds = new ArrayList<>();
            this.pathIds.add(this.road);
        }
    }

    @Override
    public String toString() {
        return originalX +
                "," + originalY +
                "," + time +
                "," + x +
                "," + y +
                "," + road;
    }
}
