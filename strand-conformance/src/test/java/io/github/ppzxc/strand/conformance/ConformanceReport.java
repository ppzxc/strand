package io.github.ppzxc.strand.conformance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/** 실행 경로별 결과와 벡터별 통합 결과를 집계한다. */
final class ConformanceReport {

    static final String CORE_PATH = "core";
    static final String NIO_PATH = "nio";
    static final String INTEGRATED_PATH = "integrated";

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
            integrated.add(integrate(id, vectorsByPath));
        }
        return forPath(INTEGRATED_PATH, integrated);
    }

    static VectorOutcome integratedOutcome(String vectorId, VectorOutcome core, VectorOutcome nio) {
        Map<String, Map<String, VectorOutcome>> byPath = new LinkedHashMap<>();
        byPath.put(CORE_PATH, Collections.singletonMap(vectorId, core));
        byPath.put(NIO_PATH, Collections.singletonMap(vectorId, nio));
        return integrate(vectorId, byPath);
    }

    private static VectorOutcome integrate(String id, Map<String, Map<String, VectorOutcome>> vectorsByPath) {
        List<String> failures = new ArrayList<>();
        List<String> unsupported = new ArrayList<>();
        boolean passed = false;
        for (Map.Entry<String, Map<String, VectorOutcome>> path : vectorsByPath.entrySet()) {
            VectorOutcome outcome = path.getValue().get(id);
            if (outcome == null) {
                continue;
            }
            if (outcome.status() == VectorOutcome.Status.FAIL) {
                failures.add(path.getKey() + ": " + String.join(", ", outcome.details()));
            } else if (outcome.status() == VectorOutcome.Status.PASS) {
                passed = true;
            } else {
                for (String detail : outcome.details()) unsupported.add(path.getKey() + ": " + detail);
            }
        }
        if (!failures.isEmpty()) {
            return VectorOutcome.fail(id, String.join("; ", failures));
        }
        if (passed) {
            return VectorOutcome.pass(id);
        }
        return VectorOutcome.unsupported(id, unsupported);
    }

    int count(VectorOutcome.Status status) {
        int count = 0;
        for (List<VectorOutcome> outcomes : byPath.values()) {
            for (VectorOutcome outcome : outcomes) {
                if (outcome.status() == status) {
                    count++;
                }
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
                    details.add(path.getKey() + "/" + outcome.vectorId() + " " + outcome.status() + " "
                            + outcome.details());
                }
            }
        }
        return "pass=" + count(VectorOutcome.Status.PASS) + ", fail=" + count(VectorOutcome.Status.FAIL)
                + ", unsupported=" + count(VectorOutcome.Status.UNSUPPORTED) + "; " + details;
    }
}
