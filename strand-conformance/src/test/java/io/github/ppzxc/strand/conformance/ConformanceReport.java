package io.github.ppzxc.strand.conformance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 실행 경로별 결과와 벡터별 통합 결과를 집계한다. */
final class ConformanceReport {

    private final Map<String, List<VectorOutcome>> byPath;

    private ConformanceReport(Map<String, List<VectorOutcome>> byPath) {
        Map<String, List<VectorOutcome>> copy = new LinkedHashMap<>();
        for (Map.Entry<String, List<VectorOutcome>> entry : byPath.entrySet()) {
            copy.put(entry.getKey(), Collections.unmodifiableList(new ArrayList<>(entry.getValue())));
        }
        this.byPath = Collections.unmodifiableMap(copy);
    }

    static ConformanceReport forPath(String path, List<VectorOutcome> outcomes) {
        Map<String, List<VectorOutcome>> result = new LinkedHashMap<>();
        result.put(path, outcomes);
        return new ConformanceReport(result);
    }

    static ConformanceReport integrated(Map<String, List<VectorOutcome>> outcomesByPath) {
        Map<String, Map<String, VectorOutcome>> vectorsByPath = new LinkedHashMap<>();
        LinkedHashSet<String> ids = new LinkedHashSet<>();
        for (Map.Entry<String, List<VectorOutcome>> path : outcomesByPath.entrySet()) {
            Map<String, VectorOutcome> byId = new LinkedHashMap<>();
            for (VectorOutcome outcome : path.getValue()) {
                byId.put(outcome.vectorId(), outcome);
                ids.add(outcome.vectorId());
            }
            vectorsByPath.put(path.getKey(), byId);
        }
        List<VectorOutcome> integrated = new ArrayList<>();
        for (String id : ids) {
            List<VectorOutcome> results = new ArrayList<>();
            for (Map<String, VectorOutcome> path : vectorsByPath.values()) {
                if (path.containsKey(id)) results.add(path.get(id));
            }
            integrated.add(integrate(id, results));
        }
        return forPath("integrated", integrated);
    }

    private static VectorOutcome integrate(String id, List<VectorOutcome> results) {
        for (VectorOutcome result : results) {
            if (result.status() == VectorOutcome.Status.FAIL) return VectorOutcome.fail(id, result.details().toString());
        }
        for (VectorOutcome result : results) {
            if (result.status() == VectorOutcome.Status.PASS) return VectorOutcome.pass(id);
        }
        List<String> unsupported = new ArrayList<>();
        for (VectorOutcome result : results) unsupported.addAll(result.details());
        return VectorOutcome.unsupported(id, unsupported);
    }

    int count(VectorOutcome.Status status) {
        int count = 0;
        for (List<VectorOutcome> outcomes : byPath.values()) {
            for (VectorOutcome outcome : outcomes) {
                if (outcome.status() == status) count++;
            }
        }
        return count;
    }

    List<VectorOutcome> outcomes() {
        List<VectorOutcome> outcomes = new ArrayList<>();
        for (List<VectorOutcome> path : byPath.values()) outcomes.addAll(path);
        return Collections.unmodifiableList(outcomes);
    }

    boolean passes() {
        return count(VectorOutcome.Status.FAIL) == 0 && count(VectorOutcome.Status.UNSUPPORTED) == 0;
    }

    String summary() {
        List<String> details = new ArrayList<>();
        for (Map.Entry<String, List<VectorOutcome>> path : byPath.entrySet()) {
            for (VectorOutcome outcome : path.getValue()) {
                if (outcome.status() != VectorOutcome.Status.PASS) {
                    details.add(path.getKey() + "/" + outcome.vectorId() + " " + outcome.status() + " " + outcome.details());
                }
            }
        }
        return "pass=" + count(VectorOutcome.Status.PASS) + ", fail=" + count(VectorOutcome.Status.FAIL)
                + ", unsupported=" + count(VectorOutcome.Status.UNSUPPORTED) + "; " + details;
    }
}
