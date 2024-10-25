package MapMatchBaseline.STMatching;

import com.bmwcarit.barefoot.matcher.*;
import com.bmwcarit.barefoot.road.Heading;
import com.bmwcarit.barefoot.roadmap.*;
import com.bmwcarit.barefoot.scheduler.StaticScheduler;
import com.bmwcarit.barefoot.scheduler.Task;
import com.bmwcarit.barefoot.spatial.Geography;
import com.bmwcarit.barefoot.spatial.SpatialOperator;
import com.bmwcarit.barefoot.topology.Cost;
import com.bmwcarit.barefoot.topology.Dijkstra;
import com.bmwcarit.barefoot.topology.Router;
import com.bmwcarit.barefoot.util.Stopwatch;
import com.bmwcarit.barefoot.util.Tuple;
import common.Region;
import common.RegionUtil;
import utils.CommonUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class EnhanceSTMatcher3_O extends Matcher {

    public final static double sqrt_2pi = Math.sqrt(2d * Math.PI);
    public final static double sigma_z = 8;//GPS误差分布标准差 单位：m

    public static AtomicLong candsNum = new AtomicLong(0);
    public static AtomicLong sampleNum = new AtomicLong(0);

    static HashMap<Long, List<Integer>> busSpeedMap = new HashMap<>();

    static {
        try {
            BufferedReader reader = new BufferedReader(new FileReader("data/459SpeedMap"));
            String str = reader.readLine();
            while (str != null && !str.isEmpty()) {
                String[] split = str.split(":");
                List<Integer> speeds = Arrays.stream(split[1].split(",")).map(Integer::valueOf).collect(Collectors.toList());
                busSpeedMap.put(Long.parseLong(split[0]), speeds);
                str = reader.readLine();
            }
        } catch (Exception e) {
        }
    }
    /**
     * Creates a HMM map matching filter for some map, router, cost function, and spatial operator.
     *
     * @param map     {@link RoadMap} object of the map to be matched to.
     * @param router  {@link Router} object to be used for route estimation.
     * @param cost    Cost function to be used for routing.
     * @param spatial Spatial operator for spatial calculations.
     */
    public EnhanceSTMatcher3_O(RoadMap map, Router<Road, RoadPoint> router, Cost<Road> cost, SpatialOperator spatial) {
        super(map, router, cost, spatial);
    }

    public EnhanceSTMatcher3_O(RoadMap map) {
        super(map, new Dijkstra<Road, RoadPoint>(), new TimePriority(), new Geography());
    }

    public EnhanceSTMatcher3_O(RoadMap map, Integer maxRadius) {
        super(map, new Dijkstra<Road, RoadPoint>(), new TimePriority(), new Geography());
        //设置候选点选取半径
        super.setMaxRadius(maxRadius);
    }

    @Override
    protected Set<Tuple<MatcherCandidate, Double>> candidates(Set<MatcherCandidate> predecessors,
                                                              MatcherSample sample) {
        //enhance1
        Region.Area area = RegionUtil.getArea(sample.point().getX(), sample.point().getY());

        //选出指定半径内sample的候选道路点集
//        Set<RoadPoint> points_ = map.spatial().radius(sample.point(), area.radius);
        Set<RoadPoint> points_ = map.spatial().radius(sample.point(), radius); // HMM-O
        candsNum.addAndGet(points_.size());
        sampleNum.incrementAndGet();
        Set<RoadPoint> points = new HashSet<>(Minset.minimize(points_));

        Map<Long, RoadPoint> map = new HashMap<>();
        for (RoadPoint point : points) {
            map.put(point.edge().id(), point);
        }

        //找到和前一阶段候选点在同一条道路上的候选点，若距离接近则替换
        for (MatcherCandidate predecessor : predecessors) {
            RoadPoint point = map.get(predecessor.point().edge().id());
            if (point != null && point.edge() != null
                    && spatial.distance(point.geometry(),
                    predecessor.point().geometry()) < getSigma()
                    && ((point.edge().heading() == Heading.forward
                    && point.fraction() < predecessor.point().fraction())
                    || (point.edge().heading() == Heading.backward
                    && point.fraction() > predecessor.point().fraction()))) {
                points.remove(point);
                points.add(predecessor.point());
            }
        }

        Set<Tuple<MatcherCandidate, Double>> candidates = new HashSet<>();

        //根据HMMM公式计算每个候选点的观测概率
        for (RoadPoint point : points) {
            double dz = spatial.distance(sample.point(), point.geometry());
            //增强1：区域GPS误差分布
            double emission = (1 / (sqrt_2pi * sigma_z)) * Math.exp((-0.5) * dz * dz / (sigma_z * sigma_z)); // HMM-O
//            double emission = area.gaussianPDF(dz);

            MatcherCandidate candidate = new MatcherCandidate(point, sample);
            candidates.add(new Tuple<>(candidate, emission));
        }

        return candidates;
    }

    @Override
    protected Map<MatcherCandidate, Map<MatcherCandidate, Tuple<MatcherTransition, Double>>> transitions(
            final Tuple<MatcherSample, Set<MatcherCandidate>> predecessors,
            final Tuple<MatcherSample, Set<MatcherCandidate>> candidates) {

        final Set<RoadPoint> targets = new HashSet<>();
        for (MatcherCandidate candidate : candidates.two()) {
            targets.add(candidate.point());
        }

        final AtomicInteger count = new AtomicInteger();
        final Map<MatcherCandidate, Map<MatcherCandidate, Tuple<MatcherTransition, Double>>> transitions =
                new ConcurrentHashMap<>();
        final double bound = Math.max(1000d, Math.min(distance,
                ((candidates.one().time() - predecessors.one().time()) / 1000) * 100));
        final double sampleDistance = spatial.distance(predecessors.one().point(), candidates.one().point());//样本点直线距离
        final long preTime = predecessors.one().time(), nowTime = candidates.one().time();
        final double travelTime = (double) (candidates.one().time() - predecessors.one().time()) / 1000;//两样本点之间的行驶时间，单位：s
        //自定义全局线程池调度器
        StaticScheduler.InlineScheduler scheduler = StaticScheduler.scheduler();
        //遍历所有前置候选点
        for (final MatcherCandidate predecessor : predecessors.two()) {
            scheduler.spawn(new Task() {
                @Override
                public void run() {
                    Map<MatcherCandidate, Tuple<MatcherTransition, Double>> map = new HashMap<>();
                    Stopwatch sw = new Stopwatch();
                    sw.start();
                    //针对每一个前置候选点，获取它到当前所有候选点的道路路径
                    Map<RoadPoint, List<Road>> routes =
                            router.route(predecessor.point(), targets, cost, new Distance(), bound);
                    sw.stop();

                    //遍历当前候选点集
                    for (MatcherCandidate candidate : candidates.two()) {
                        List<Road> edges = routes.get(candidate.point());

                        if (edges == null) {
                            continue;
                        }

                        //获取 前置候选点 -> 当前候选点 路由
                        Route route = new Route(predecessor.point(), candidate.point(), edges);

                        if (shortenTurns() && edges.size() >= 2) {
                            if (edges.get(0).base().id() == edges.get(1).base().id()
                                    && edges.get(0).id() != edges.get(1).id()) {
                                RoadPoint start = predecessor.point(), end = candidate.point();
                                if (edges.size() > 2) {
                                    start = new RoadPoint(edges.get(1), 1 - start.fraction());
                                    edges.remove(0);
                                } else {
                                    // Here, additional cost of 5 meters are added to the route
                                    // length in order to penalize and avoid turns, e.g., at the end
                                    // of a trace.
                                    if (start.fraction() < 1 - end.fraction()) {
                                        end = new RoadPoint(edges.get(0), Math.min(1d,
                                                1 - end.fraction() + (5d / edges.get(0).length())));
                                        edges.remove(1);
                                    } else {
                                        start = new RoadPoint(edges.get(1), Math.max(0d, 1
                                                - start.fraction() - (5d / edges.get(1).length())));
                                        edges.remove(0);
                                    }
                                }
                                route = new Route(start, end, edges);
                            }
                        }


                        //根据ST-Matching公式计算每个候选点间的状态转移概率
                        double routeDistance = route.cost(new Distance());//候选路径距离
                        double Fs = sampleDistance / routeDistance;//空间分析函数值
                        double aver_v = routeDistance * 3.6 / travelTime;//候选路径平均行驶速率 单位：km/h
                        double x = 0, y = 0;
                        for (Road road : edges) {
//                            double e_v = Math.min(road.maxspeed(), 130);
                            //增强2：使用路段实时速率替换
                            double e_v = getRoadSpeed(road, preTime, nowTime);
                            x += e_v*aver_v;
                            y += e_v*e_v;
                        }
                        double Ft = x / (Math.sqrt(y) * Math.sqrt(edges.size()) * aver_v);//时间分析函数值
                        double transition = Fs * Ft;//状态转移概率
                        map.put(candidate, new Tuple<>(new MatcherTransition(route), transition));

                        count.incrementAndGet();
                    }

                    transitions.put(predecessor, map);
                }
            });
        }
        if (!scheduler.sync()) {
            throw new RuntimeException();
        }

        return transitions;
    }

    public double getRoadSpeed(Road road, long start_time, long end_time) {
        int start = CommonUtils.getTimeSlot(start_time), end = CommonUtils.getTimeSlot(end_time);
        List<Integer> list = busSpeedMap.get(road.base().refid());
        if (list != null) {
            int speedSum = 0, count = 0;
            for (int i = start; i <= end; i++) {
                if (list.get(i) != -1) {
                    speedSum += list.get(i);
                    count++;
                }
            }
            if (count != 0) return (double) speedSum / count;
        }
        return Math.min(road.maxspeed(), 130);
    }

    /**
     * 根据前一阶段样本点及其候选点集 获取当前样本点的候选点集并计算相关概率
     */
    public Set<MatcherCandidate> execute(Set<MatcherCandidate> predecessors, MatcherSample previous, MatcherSample sample) {
        assert (predecessors != null);
        assert (sample != null);

        Set<MatcherCandidate> result = new HashSet<>();
        //计算候选点观测概率：Set<Tuple<候选点, 观测概率>>
        Set<Tuple<MatcherCandidate, Double>> candidates = candidates(predecessors, sample);

        if (!predecessors.isEmpty()) {
            Set<MatcherCandidate> states = new HashSet<>();
            for (Tuple<MatcherCandidate, Double> candidate : candidates) {
                states.add(candidate.one());
            }

            //计算前一阶段每个候选点转移到当前阶段各候选点概率：Map<前阶段候选点，Map<当前阶段候选点，<转移路由，状态转移概率>>
            Map<MatcherCandidate, Map<MatcherCandidate, Tuple<MatcherTransition, Double>>> transitions =
                    transitions(new Tuple<>(previous, predecessors), new Tuple<>(sample, states));

            //遍历当前阶段候选点
            for (Tuple<MatcherCandidate, Double> candidate : candidates) {
                MatcherCandidate candidate_ = candidate.one();
                candidate_.seqprob(Double.NEGATIVE_INFINITY);

                double maxProb = -1;
                MatcherCandidate maxPredecessor = null;
                //对于每个当前候选点，遍历前候选点，查找能转移到它的前候选点，计算最大概率，并将最大概率前候选点设为前置点
                for (MatcherCandidate predecessor : predecessors) {
                    //从predecessor到candidate的transition：<转移路由，状态转移概率>
                    Tuple<MatcherTransition, Double> transition = transitions.get(predecessor).get(candidate_);

                    if (transition == null || transition.two() == 0) {
                        continue;
                    }
                    //到达c的概率 = 前候选点到达概率 + 转移到c的概率*c的观测概率
                    double prob = predecessor.filtprob() +  transition.two()*candidate.two();
                    if (prob > maxProb) {
                        maxProb = prob;
                        maxPredecessor = predecessor;
                    }
                }

                if (maxProb < 0) {
                    continue;
                }

                candidate_.filtprob(maxProb);
                candidate_.predecessor(maxPredecessor);

                result.add(candidate_);
            }
        }

        //若前一阶段没有一个候选点可以转移到当前阶段，或者前阶段候选点为空 即此时为初始点
        if (result.isEmpty() || predecessors.isEmpty()) {
            for (Tuple<MatcherCandidate, Double> candidate : candidates) {
                if (candidate.two() == 0) {
                    continue;
                }
                MatcherCandidate candidate_ = candidate.one();
                //将到达概率设为观测概率
                candidate_.filtprob(candidate.two());
                candidate_.seqprob(Math.log10(candidate.two()));
                result.add(candidate_);

            }
        }

        return result;
    }
}
