package bfGpsDataMatch;

import MapMatchBaseline.busLineMatch.BusLineMatcher;
import com.bmwcarit.barefoot.matcher.Matcher;
import com.bmwcarit.barefoot.matcher.MatcherCandidate;
import com.bmwcarit.barefoot.matcher.MatcherKState;
import com.bmwcarit.barefoot.matcher.MatcherSample;
import com.bmwcarit.barefoot.road.BfmapReader;
import com.bmwcarit.barefoot.roadmap.RoadMap;
import com.bmwcarit.barefoot.util.Tuple;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * deploy-mode client模式下使用的可广播泛型Matcher。读取driver端文件系统路径下的map文件
 * @param <M>
 */
public class ClientMapMatcher<M extends Matcher> implements Serializable {

    private final RoadMap map;
    private int maxRadius = -1;
    private final Class<M> matcherClass;//由于泛型擦除，在ClientMapMatcher对象中实际上并没有保存M的实际类型，故无法直接创建M类型的对象，故通过成员变量的方式来保存泛型的实际类型的Class对象
    private HashMap<String, HashSet<Long>> busLineMap;

    public ClientMapMatcher(RoadMap map, int maxRadius, Class<M> matcherClass) {
        this.map = map;
        this.maxRadius = maxRadius;
        this.matcherClass = matcherClass;
    }

    public ClientMapMatcher(String mapPath, Class<M> matcherClass) {
        this.map = RoadMap.Load(new BfmapReader(mapPath));
        this.matcherClass = matcherClass;
    }

    public ClientMapMatcher(String mapPath, int maxRadius, Class<M> matcherClass) {
        this.map = RoadMap.Load(new BfmapReader(mapPath));
        this.maxRadius = maxRadius;
        this.matcherClass = matcherClass;
        if (matcherClass == BusLineMatcher.class) {
            busLineMap = BusLineMatcher.loadBusLine();
        }
    }

    //赋值指令可能会被重排序到初始化指令前面，从而导致在多线程情境下第一个拿到锁的线程执行了对matcherInstance的赋值指令，
    //但还没来得及执行初始化指令，而另一个线程就拿到了不为null但是还未初始化完成的matcherInstance去使用而导致出错。因此需要用volatile修饰防止指令重排序。
    private volatile M matcherInstance;

    /**
     * 双重检测锁（DoubleCheckLockSingleton, DCL），当分发到Executor后被使用时再进行初始化
     */
    private M getMatcher() throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (matcherInstance == null) {
            synchronized(this) {
                if (matcherInstance == null) { // initialize map matcher once per Executor (JVM process/cluster node)
                    map.construct();
                    if (maxRadius == -1)
                        matcherInstance = matcherClass.getDeclaredConstructor(RoadMap.class).newInstance(map);
                    else if (busLineMap == null)
                        matcherInstance = matcherClass.getDeclaredConstructor(RoadMap.class, Integer.class).newInstance(map, maxRadius);
                    else
                        matcherInstance = matcherClass.getDeclaredConstructor(RoadMap.class, Integer.class, HashMap.class).newInstance(map, maxRadius, busLineMap);
                }
            }
        }
        return matcherInstance;
    }

    public MatcherKState mmatch(List<MatcherSample> samples) {
        return mmatch(samples, 0, 0);
    }

    private MatcherKState mmatch(List<MatcherSample> samples, double minDistance, int minInterval) {
        try {
            return getMatcher().mmatch(samples, minDistance, minInterval);
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Tuple<List<MatcherSample>, List<MatcherCandidate>> getMatchSequence(List<MatcherSample> samples) {
        try {
            return getMatcher().getMatchSequence(samples);
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<MatcherCandidate> getMatchCandidates(List<MatcherSample> samples) {
        try {
            return getMatcher().getMatchCandidates(samples);
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }
}
