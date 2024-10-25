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

package com.bmwcarit.barefoot.matcher;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.bmwcarit.barefoot.roadmap.Road;
import com.bmwcarit.barefoot.roadmap.RoadPoint;

/**
 * Minimizes a set of matching candidates represented as {@link RoadPoint} to remove semantically
 * redundant candidates.
 */
public abstract class Minset {
    /**
     * Floating point precision for considering a {@link RoadPoint} be the same as a vertex,
     * fraction is zero or one (default: 1E-8).
     */
    public static double precision = 1E-8;

    /**
     * 四舍五入保留8位小数
     */
    private static double round(double value) {
        return Math.round(value / precision) * precision;
    }

    /**
     * Removes semantically redundant matching candidates from a set of matching candidates (as
     * {@link RoadPoint} object) and returns a minimized (reduced) subset.
     * <p>
     * Given a position measurement, a matching candidate is each road in a certain radius of the
     * measured position, and in particular that point on each road that is closest to the measured
     * position. Hence, there are as many state candidates as roads in that area. The idea is to
     * conserve only possible routes through the area and use each route with its closest point to
     * the measured position as a matching candidate. Since roads are split into multiple segments,
     * the number of matching candidates is significantly higher than the respective number of
     * routes. To give an example, assume the following matching candidates as {@link RoadPoint}
     * objects with a road id and a fraction:
     *
     * <ul>
     * <li><i>(r<sub>i</sub>, 0.5)</i>
     * <li><i>(r<sub>j</sub>, 0.0)</i>
     * <li><i>(r<sub>k</sub>, 0.0)</i>
     * </ul>
     *
     * where they are connected as <i>r<sub>i</sub> &#8594; r<sub>j</sub></i> and <i>r<sub>i</sub>
     * &#8594; r<sub>k</sub></i>. Here, matching candidates <i>r<sub>j</sub></i> and
     * <i>r<sub>k</sub></i> can be removed if we see routes as matching candidates. This is because
     * both, <i>r<sub>j</sub></i> and <i>r<sub>k</sub></i>, are reachable from <i>r<sub>i</sub></i>.
     * <p>
     * <b>Note:</b> Of course, <i>r<sub>j</sub></i> and <i>r<sub>k</sub></i> may be seen as relevant
     * matching candidates, however, in the present HMM map matching algorithm there is no
     * optimization of matching candidates along the road, instead it only considers the closest
     * point of a road as a matching candidate.
     *
     * @param candidates Set of matching candidates as {@link RoadPoint} objects.
     * @return Minimized (reduced) set of matching candidates as {@link RoadPoint} objects.
     *
     * 翻译：
     * 从匹配候选点集合（作为 RoadPoint 对象）中删除语义冗余的匹配候选点，并返回最小化（缩减）子集。给定一个位置测量值，
     * 匹配候选点就是测量位置一定半径范围内的每条道路，尤其是每条道路上最靠近测量位置的那一点。因此，该区域有多少条道路，就有多少个候选状态。
     * 这样做的目的是只保留通过该区域的可能路线，并将每条路线上距离测量位置最近的点作为匹配候选点。
     * 由于道路被分成多段，因此匹配候选道路的数量远远高于相应的路线数量。举例说明，假设以下匹配候选点是带有道路 ID 和分数的 RoadPoint 对象：
     * (ri, 0.5)
     * (rj, 0.0)
     * (rk, 0.0)
     * 它们的连接方式为 ri → rj 和 ri → rk。在这里，如果我们将路由视为匹配候选者，则可以删除匹配候选者 rj 和 rk。这是因为 rj 和 rk 都可以从 ri 到达。
     * 注意：当然，rj和rk可以被视为相关匹配候选，但是，在现有的HMM地图匹配算法中，没有对沿道路的匹配候选进行优化，而是仅将道路上最近的点视为匹配候选。
     *
     * 分析：
     * 这个方法的大概意思是说 由于一条道路被划分为多个路段（即一个OSMID 有不同的source和target的路段）, 那么对于一条实际的道路，除了最近距离匹配点外，还会有多个路段端点匹配候选点
     * 为避免重复，过滤所有其他候选点可到达的路段端点（即fraction值为0或1）。但这可能会导致当一个定位点最近匹配点就是端点时，这个较为重要的候选点反而被过滤
     */
    public static Set<RoadPoint> minimize(Set<RoadPoint> candidates) {

        HashMap<Long, RoadPoint> map = new HashMap<>();
        HashMap<Long, Integer> misses = new HashMap<>();
        HashSet<Long> removes = new HashSet<>();

        for (RoadPoint candidate : candidates) {
            map.put(candidate.edge().id(), candidate);
            misses.put(candidate.edge().id(), 0);
        }

        for (RoadPoint candidate : candidates) {
            Iterator<Road> successors = candidate.edge().successors();
            Long id = candidate.edge().id();

            //遍历候选道路的所有后继边
            while (successors.hasNext()) {
                Road successor = successors.next();

                //若后继边不在候选道路集中，则候选边misses值增1
                if (!map.containsKey(successor.id())) {
                    misses.put(id, misses.get(id) + 1);
                }

                //若后继边在候选道路集中 且此后继的候选点在端点上，则将此后继边加入拟移除集，候选边misses值增1
                if (map.containsKey(successor.id())
                        && round(map.get(successor.id()).fraction()) == 0) {
                    removes.add(successor.id());
                    misses.put(id, misses.get(id) + 1);
                }
            }
        }

        for (RoadPoint candidate : candidates) {
            Long id = candidate.edge().id();
            //如果一个候选点在道路右端点，且它的后继边都是正常候选边，也就是说这个候选路段是可以省略的
            if (map.containsKey(id) && !removes.contains(id) && round(candidate.fraction()) == 1
                    && misses.get(id) == 0) {
                removes.add(id);
            }
        }

        for (Long id : removes) {
            map.remove(id);
        }

        return new HashSet<>(map.values());
    }
}
