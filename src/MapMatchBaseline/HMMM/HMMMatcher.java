package MapMatchBaseline.HMMM;

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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class HMMMatcher extends Matcher {

    public final static double sqrt_2pi = Math.sqrt(2d * Math.PI);
    public final static double sigma_z = 4.07;//GPS误差分布标准差 单位：m

    public static AtomicLong candsNum = new AtomicLong(0);
    public static AtomicLong sampleNum = new AtomicLong(0);

    //状态转移公式参数
    public final static double beta = 48.0;//单位：m

    /**
     * Creates a HMM map matching filter for some map, router, cost function, and spatial operator.
     *
     * @param map     {@link RoadMap} object of the map to be matched to.
     * @param router  {@link Router} object to be used for route estimation.
     * @param cost    Cost function to be used for routing.
     * @param spatial Spatial operator for spatial calculations.
     */
    public HMMMatcher(RoadMap map, Router<Road, RoadPoint> router, Cost<Road> cost, SpatialOperator spatial) {
        super(map, router, cost, spatial);
    }

    public HMMMatcher(RoadMap map) {
        super(map, new Dijkstra<Road, RoadPoint>(), new TimePriority(), new Geography());
    }

    public HMMMatcher(RoadMap map, Integer maxRadius) {
        super(map, new Dijkstra<Road, RoadPoint>(), new TimePriority(), new Geography());
        //设置候选点选取半径
        super.setMaxRadius(maxRadius);
    }

    @Override
    protected Set<Tuple<MatcherCandidate, Double>> candidates(Set<MatcherCandidate> predecessors,
                                                              MatcherSample sample) {

        //选出指定半径内sample的候选道路点集
        Set<RoadPoint> points_ = map.spatial().radius(sample.point(), radius);
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
            double emission = (1 / (sqrt_2pi * sigma_z)) * Math.exp((-0.5) * dz * dz / (sigma_z * sigma_z));

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
        final double sampleDistance = spatial.distance(predecessors.one().point(), candidates.one().point());
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


                        //根据HMMM公式计算每个候选点间的状态转移概率
                        double routeDistance = route.cost(new Distance());
                        double dt = Math.abs(sampleDistance - routeDistance);
                        double transition = (1 / beta) * Math.exp((-1.0) * dt / beta);

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
                    //transition：<转移路由，状态转移概率>
                    Tuple<MatcherTransition, Double> transition = transitions.get(predecessor).get(candidate_);

                    if (transition == null || transition.two() == 0) {
                        continue;
                    }

                    double prob = predecessor.filtprob() * transition.two();
                    if (prob > maxProb) {
                        maxProb = prob;
                        maxPredecessor = predecessor;
                    }
                }

                if (maxProb < 0) {
                    continue;
                }

                candidate_.filtprob(maxProb * candidate.two());
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
