package MapMatchBaseline.busLineMatch;

import com.bmwcarit.barefoot.matcher.*;
import com.bmwcarit.barefoot.road.Heading;
import com.bmwcarit.barefoot.roadmap.*;
import com.bmwcarit.barefoot.scheduler.StaticScheduler;
import com.bmwcarit.barefoot.scheduler.Task;
import com.bmwcarit.barefoot.spatial.Geography;
import com.bmwcarit.barefoot.topology.Dijkstra;
import com.bmwcarit.barefoot.util.Stopwatch;
import com.bmwcarit.barefoot.util.Tuple;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class BusLineMatcher extends Matcher implements Serializable {

    private HashMap<String, HashSet<Long>> busLineMap;

    public static HashMap<String, HashSet<Long>> loadBusLine() {
        try {
            HashMap<String, HashSet<Long>> busLineMap = new HashMap<>();
            BufferedReader reader = new BufferedReader(new FileReader("data/busLineRoads"));
            String str = reader.readLine();
            while (str != null && !str.isEmpty()) {
                String[] split = str.split(":");
                HashSet<Long> lineRoads = (HashSet<Long>) Arrays.stream(split[1].split(",")).map(Long::valueOf).collect(Collectors.toSet());
                busLineMap.put(split[0], lineRoads);
                str = reader.readLine();
            }
            System.out.println("数据加载完毕，共" + busLineMap.size() + "条公交线路");
            return busLineMap;
        } catch (IOException e) {
            System.out.println("数据加载错误！！！！！！！");
            e.printStackTrace();
        }
        return null;
    }

    public BusLineMatcher(RoadMap map, Integer maxRadius) {
        super(map, new Dijkstra<Road, RoadPoint>(), new TimePriority(), new Geography());
        //设置候选点选取半径
        super.setMaxRadius(maxRadius);
    }

    public BusLineMatcher(RoadMap map, Integer maxRadius, HashMap<String, HashSet<Long>> busLineMap) {
        super(map, new Dijkstra<Road, RoadPoint>(), new TimePriority(), new Geography());
        //设置候选点选取半径
        super.setMaxRadius(maxRadius);
        this.busLineMap = busLineMap;
    }


    /**
     * 判断对应公交线路是否包含了候选道路
     */
    private boolean busContainRoad(RoadPoint rp, MatcherSample sample) {
        String lineId = sample.id().split(":")[0];
        HashSet<Long> lineRoads = busLineMap.get(lineId);
        return lineRoads != null && lineRoads.contains(rp.edge().base().refid());
    }

    /**
     * 判断对应公交线路是否包含了roads中所有道路
     */
    private boolean busContainRoadList(List<Road> roads, MatcherSample sample) {
        if (roads == null) return false;
        String lineId = sample.id().split(":")[0];
        HashSet<Long> lineRoads = busLineMap.get(lineId);
        if (lineRoads == null) return false;
        for (Road road : roads) {
            if (!lineRoads.contains(road.base().refid()))
                return false;
        }
        return true;
    }

    @Override
    protected Set<Tuple<MatcherCandidate, Double>> candidates(Set<MatcherCandidate> predecessors,
                                                              MatcherSample sample) {
        //选出指定半径内sample的候选道路点集
        Set<RoadPoint> points_ = map.spatial().radius(sample.point(), radius);
        Set<RoadPoint> points = new HashSet<>(Minset.minimize(points_));
        //过滤非公交线路道路
        points.removeIf(rp -> !busContainRoad(rp, sample));

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

        //计算每个候选点的观测概率
        for (RoadPoint point : points) {
            double dz = spatial.distance(sample.point(), point.geometry());
            double emission = 1 / sqrt_2pi_sig2 * Math.exp((-1) * dz * dz / (2 * sig2));
            if (!Double.isNaN(sample.azimuth())) {
                double da = sample.azimuth() > point.azimuth()
                        ? Math.min(sample.azimuth() - point.azimuth(),
                        360 - (sample.azimuth() - point.azimuth()))
                        : Math.min(point.azimuth() - sample.azimuth(),
                        360 - (point.azimuth() - sample.azimuth()));
                emission *=
                        Math.max(1E-2, 1 / sqrt_2pi_sigA * Math.exp((-1) * da * da / (2 * sigA)));
            }

            MatcherCandidate candidate = new MatcherCandidate(point, sample);
            candidates.add(new Tuple<>(candidate, emission));
        }

        return candidates;
    }

    @Override
    protected Tuple<MatcherTransition, Double> transition(
            Tuple<MatcherSample, MatcherCandidate> predecessor,
            Tuple<MatcherSample, MatcherCandidate> candidate) {

        return null;
    }

    @Override
    protected Map<MatcherCandidate, Map<MatcherCandidate, Tuple<MatcherTransition, Double>>> transitions(
            final Tuple<MatcherSample, Set<MatcherCandidate>> predecessors,
            final Tuple<MatcherSample, Set<MatcherCandidate>> candidates) {

        MatcherSample sample = predecessors.one();
        Stopwatch sw = new Stopwatch();
        sw.start();

        final Set<RoadPoint> targets = new HashSet<>();
        for (MatcherCandidate candidate : candidates.two()) {
            targets.add(candidate.point());
        }

        final AtomicInteger count = new AtomicInteger();
        final Map<MatcherCandidate, Map<MatcherCandidate, Tuple<MatcherTransition, Double>>> transitions =
                new ConcurrentHashMap<>();
        //候选路径最大搜索长度：max(1km, min(15km, 耗时*100m/s))
        final double bound = Math.max(1000d, Math.min(distance,
                ((candidates.one().time() - predecessors.one().time()) / 1000) * 100));

        //自定义全局线程池调度器
        StaticScheduler.InlineScheduler scheduler = StaticScheduler.scheduler();
        for (final MatcherCandidate predecessor : predecessors.two()) {
            scheduler.spawn(new Task() {
                @Override
                public void run() {
                    Map<MatcherCandidate, Tuple<MatcherTransition, Double>> map = new HashMap<>();
                    //针对每一个前置候选点，用迪杰斯特拉算法获取它到当前所有候选点的道路路径
                    Map<RoadPoint, List<Road>> routes =
                            router.route(predecessor.point(), targets, cost, new Distance(), bound);

                    //过滤包含非公交线路的路径
                    routes.entrySet().removeIf(entry -> !busContainRoadList(entry.getValue(), sample));

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

                        double beta = lambda == 0
                                ? Math.max(1d, candidates.one().time() - predecessors.one().time())
                                / 1000
                                : 1 / lambda;

                        double transition = (1 / beta)
                                * Math.exp((-1.0) * route.cost(new TimePriority()) / beta);

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

        sw.stop();

        return transitions;
    }

    /**
     * 重写 简化原mmatch方法
     */
    @Override
    public MatcherKState mmatch(List<MatcherSample> samples, double minDistance, int minInterval) {
        return mmatch(samples);
    }

    public MatcherKState mmatch(List<MatcherSample> samples) {
        MatcherKState state = new MatcherKState(-1,-1);
        for (int i=0;i < samples.size();i++) {
            MatcherSample sample = samples.get(i);
            Set<MatcherCandidate> vector = execute(state.vector(), state.sample(), sample);
            state.update(vector, sample);
        }
        return state;
    }
}
