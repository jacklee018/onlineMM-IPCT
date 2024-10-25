/*
 * Copyright (C) 2015, BMW Car IT GmbH
 *
 * Author: Sebastian Mattheis <sebastian.mattheis@bmw-carit.de>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package com.bmwcarit.barefoot.roadmap;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import com.bmwcarit.barefoot.util.Tuple;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.bmwcarit.barefoot.spatial.Geography;
import com.bmwcarit.barefoot.spatial.SpatialOperator;
import com.bmwcarit.barefoot.topology.Path;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polyline;

/**
 * Route in a {@link Road} network that consists of a start and end {@link RoadPoint} and sequence
 * of {@link Road}s.
 */
public class Route extends Path<Road> {
    private final static SpatialOperator spatial = new Geography();

    private Double length = null;
    private Double time = null;

    /**
     * Creates a {@link Route} object. (This is a base case that consists of only one
     * {@link RoadPoint}.)
     *
     * @param point {@link RoadPoint} that is start and end point.
     */
    public Route(RoadPoint point) {
        super(point);
    }

    /**
     * Creates a {@link Route} object.
     *
     * @param source Source/start {@link RoadPoint} of the route.
     * @param target Target/end {@link RoadPoint} of the route.
     * @param roads Sequence of {@link Road}s that make the route.
     */
    public Route(RoadPoint source, RoadPoint target, List<Road> roads) {
        super(source, target, roads);
    }

    /**
     * @return 获取这条路径上指定长度比例位置的RoadPoint，并将原路由拆分为前后两部分
     */
    public Tuple<Tuple<Route, Route>, RoadPoint> getPointAtFraction(double fracInPath) {
        if (Double.isNaN(fracInPath) || fracInPath < 0 || fracInPath > 1) throw new IllegalArgumentException(fracInPath+"");
        if (edges.isEmpty()) throw new RuntimeException("Route的edges为空");
        //避免计算精度问题，为端点时直接构造返回
        if (fracInPath == 0) return createHalfRoute(source(), new RoadPoint(source().edge(), source().fraction()), target(), edges.subList(0,1), edges.subList(0, edges.size())); //Route的构造中会复制edges，所以这里可以这样写
        if (fracInPath == 1) return createHalfRoute(source(), new RoadPoint(target().edge(), target().fraction()), target(), edges.subList(0,edges.size()), edges.subList(edges.size()-1, edges.size()));
        double pathLength = length(); //length函数返回路径实际总长度
        double targetLength = fracInPath * pathLength; //目标长度
        // 只有一条道路的情况
        if (edges.size() == 1) {
            Road road = edges.getFirst();
            double roadLen = CostInstance.DISTANCE.cost(road);
            double fraction =  Math.min(1, (roadLen * source().fraction() + targetLength) / roadLen); //原路由长度为source→target，故这里的fraction这样计算
            if (fraction < source().fraction()) fraction = source().fraction();
            if (fraction > target().fraction()) fraction = target().fraction();
            RoadPoint rp = new RoadPoint(road, fraction);
            return createHalfRoute(source(), rp, target(), edges, edges);
        }
        // 有多条道路的情况
        double accumulatedLength = 0.0;
        for (int i = 0; i < edges.size(); i++) {
            Road currentRoad = edges.get(i);
            double currentRoadLength = CostInstance.DISTANCE.cost(currentRoad);

            // 调整第一条和最后一条道路的实际路由长度
            if (i == 0) {
                currentRoadLength = currentRoadLength * (1 - source().fraction());
            } else if (i == edges.size() - 1) {
                currentRoadLength = currentRoadLength * target().fraction();
            }

            // 检查目标点是否在当前道路上
            if (accumulatedLength + currentRoadLength >= targetLength) {
                double fraction;
                if (i == 0) {
                    fraction = (targetLength / CostInstance.DISTANCE.cost(currentRoad)) + source().fraction();
                } else {
                    // 对于中间或最后一条道路，直接计算目标长度占当前道路的比例
                    double lengthIntoCurrentRoad = targetLength - accumulatedLength;
                    fraction = lengthIntoCurrentRoad / CostInstance.DISTANCE.cost(currentRoad);
                }
                RoadPoint rp = new RoadPoint(currentRoad, Math.min(fraction, 1)); //从rp处拆分路由
                return createHalfRoute(source(), rp, target(), edges.subList(0, i+1), edges.subList(i, edges.size()));
            }
            accumulatedLength += currentRoadLength;
        }

        // 如果循环结束也没有返回，那么说明fraction=1，目标点就是路径的终点
        RoadPoint rp = new RoadPoint(target().edge(), target().fraction());
        return createHalfRoute(source(), rp, target(), edges, edges.subList(edges.size()-1, edges.size()));
    }

    private Tuple<Tuple<Route, Route>, RoadPoint> createHalfRoute(RoadPoint source, RoadPoint rp, RoadPoint target, List<Road> leftEdges, List<Road> rightEdges) {
        Route preRoute, postRoute;
        preRoute = new Route(new RoadPoint(source.edge(), source.fraction()), rp, leftEdges);
        postRoute = new Route(rp, new RoadPoint(target.edge(), target.fraction()), rightEdges);
        return new Tuple<>(new Tuple<>(preRoute, postRoute), rp);
    }

    /**
     * Gets size of the route, i.e. the number of {@link Road}s that make the route.
     *
     * @return Number of the {@link Road}s that make the route.
     */
    public int size() {
        return path().size();
    }

    /**
     * Gets {@link Road} by index of the sequence of {@link Road} objects.
     *
     * @param index Index of the road to be returned from the sequence of roads.
     * @return Road at the given index.
     */
    public Road get(int index) {
        return path().get(index);
    }

    /**
     * Gets length of the {@link Route} in meters, uses cost function {@link Distance} to determine
     * the length.
     *
     * @return Length of the {@link Route} in meters.
     */
    public double length() {
        if (length != null) {
            return length;
        } else {
            length = this.cost(new Distance());
            return length;
        }
    }

    /**
     * Gets travel time for the {@link Route} in seconds, uses cost function {@link Time} to
     * determine travel time.
     *
     * @return Travel time of the {@link Route} in seconds.
     */
    public double time() {
        if (time != null) {
            return time;
        } else {
            time = cost(new Time());
            return time;
        }
    }

    @Override
    public RoadPoint source() {
        return (RoadPoint) super.source();
    }

    @Override
    public RoadPoint target() {
        return (RoadPoint) super.target();
    }

    @Override
    public boolean add(Path<Road> other) {
        time = null;
        length = null;
        return super.add(other);
    }

    /**
     * Gets geometry of the {@link Route} from start point to end point by concatenating the
     * geometries of the roads in the route's road sequence respectively.
     *
     * @return Geometry of the route.
     */
    public Polyline geometry() {
        Polyline geometry = new Polyline();

        geometry.startPath(source().geometry());

        if (source().edge().id() != target().edge().id()) {
            {
                double f = source().edge().length() * source().fraction(), s = 0;
                Point a = source().edge().geometry().getPoint(0);

                for (int i = 1; i < source().edge().geometry().getPointCount(); ++i) {
                    Point b = source().edge().geometry().getPoint(i);
                    s += spatial.distance(a, b);
                    a = b;

                    if (s <= f) {
                        continue;
                    }

                    geometry.lineTo(b);
                }
            }
            for (int i = 1; i < path().size() - 1; ++i) {
                Polyline segment = path().get(i).geometry();

                for (int j = 1; j < segment.getPointCount(); ++j) {
                    geometry.lineTo(segment.getPoint(j));
                }
            }
            {
                double f = target().edge().length() * target().fraction(), s = 0;
                Point a = target().edge().geometry().getPoint(0);

                for (int i = 1; i < target().edge().geometry().getPointCount() - 1; ++i) {
                    Point b = target().edge().geometry().getPoint(i);
                    s += spatial.distance(a, b);
                    a = b;

                    if (s >= f) {
                        break;
                    }

                    geometry.lineTo(b);
                }
            }
        } else {
            double sf = source().edge().length() * source().fraction();
            double tf = target().edge().length() * target().fraction();
            double s = 0;
            Point a = source().edge().geometry().getPoint(0);

            for (int i = 1; i < source().edge().geometry().getPointCount() - 1; ++i) {
                Point b = source().edge().geometry().getPoint(i);
                s += spatial.distance(a, b);
                a = b;

                if (s <= sf) {
                    continue;
                }
                if (s >= tf) {
                    break;
                }

                geometry.lineTo(b);
            }
        }

        geometry.lineTo(target().geometry());

        return geometry;
    }

    /**
     * Creates a {@link Route} object from its JSON representation.
     *
     * @param json JSON representation of the {@link Route}.
     * @param map {@link RoadMap} object as the reference of {@link RoadPoint}s and {@link Road}s.
     * @return {@link Route} object.
     * @throws JSONException thrown on JSON extraction or parsing error.
     */
    public static Route fromJSON(JSONObject json, RoadMap map) throws JSONException {
        LinkedList<Road> roads = new LinkedList<>();

        JSONObject jsontarget = json.getJSONObject("target");
        JSONObject jsonsource = json.getJSONObject("source");
        RoadPoint target = RoadPoint.fromJSON(jsontarget, map);
        RoadPoint source = RoadPoint.fromJSON(jsonsource, map);

        JSONArray jsonroads = json.getJSONArray("roads");

        for (int j = 0; j < jsonroads.length(); ++j) {
            JSONObject jsonroad = jsonroads.getJSONObject(j);
            roads.add(Road.fromJSON(jsonroad, map));
        }

        return new Route(source, target, roads);
    }

    /**
     * Gets a JSON representation of the {@link Route}.
     *
     * @return {@link JSONObject} object.
     * @throws JSONException thrown on JSON extraction or parsing error.
     */
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        json.put("target", target().toJSON());
        json.put("source", source().toJSON());

        JSONArray jsonroads = new JSONArray();
        for (Road road : path()) {
            jsonroads.put(road.toJSON());
        }

        json.put("roads", jsonroads);

        return json;
    }
}
