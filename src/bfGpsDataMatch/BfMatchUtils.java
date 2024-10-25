package bfGpsDataMatch;

import com.bmwcarit.barefoot.matcher.MatcherCandidate;
import com.bmwcarit.barefoot.matcher.MatcherKState;
import com.bmwcarit.barefoot.matcher.MatcherSample;
import com.bmwcarit.barefoot.util.Tuple;
import common.GpsMatchPoint;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class BfMatchUtils {

    public static List<GpsMatchPoint> convertMatcherCandidates(List<MatcherCandidate> candidates) {
        if (candidates == null) {
            candidates = new ArrayList<MatcherCandidate>();
        }
        return candidates.stream().map(x -> new GpsMatchPoint(x.point())).collect(Collectors.toList());
    }

    public static List<GpsMatchPoint> convertMatcherCandidates(MatcherKState matcherKState) {
        List<Tuple<MatcherCandidate, MatcherSample>> tuples = matcherKState.sequence2();
        List<GpsMatchPoint> resultList = new ArrayList<>();
        for (Tuple<MatcherCandidate, MatcherSample> t : tuples) {
            resultList.add(new GpsMatchPoint(t));
        }
        return resultList;
    }

    public static List<List<HmmPoint>> getHmmPointsFromState(MatcherKState matcherKState) {
        List<List<HmmPoint>> result = new ArrayList<>();
        int i = 0;
        for(Tuple<Set<MatcherCandidate>,MatcherSample> tuple : matcherKState.sequence) {
            List<HmmPoint> oneVectorResult = new ArrayList<>();
            for (MatcherCandidate candidate : tuple.one()) {
                HmmPoint hmmPoint = new HmmPoint();
                hmmPoint.id = candidate.id();
                hmmPoint.point = candidate.point().geometry();
                hmmPoint.filterProb = candidate.filtprob();
                if (i > 0) {
                    for (int j = 0;j < result.get(i-1).size();j++) {
                        HmmPoint prevPoint = result.get(i - 1).get(j);
                        if (candidate.predecessor().id().equals(prevPoint.id)) {
                            hmmPoint.preIndex = j;
                        }
                    }
                }
                oneVectorResult.add(hmmPoint);
            }
            result.add(oneVectorResult);
            i++;
        }
        return result;
    }
}
