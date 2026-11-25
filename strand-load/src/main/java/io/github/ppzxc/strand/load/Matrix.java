package io.github.ppzxc.strand.load;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class Matrix {
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    private static final List<Integer> CORE_CONNECTIONS = List.of(1, 100);
    private static final List<Integer> CORE_BODIES = List.of(64, 1024);
    private static final List<Integer> EXTENDED_CONNECTIONS = List.of(1, 100, 1000);
    private static final List<Integer> EXTENDED_BODIES = List.of(64, 1024, 65_536);
    private static final List<String> TRANSPORTS = List.of("nio", "netty");

    private static final List<String> BINDING_TRANSPORTS = List.of("netty-tcp", "netty-ws");

    private final List<Integer> connections;
    private final List<Integer> bodies;
    private final List<String> transports;
    private final boolean extended;
    private final boolean planOnly;
    private final String warmup;
    private final String measure;
    private final String repetitions;
    private final String output;
    private final PrintStream out;

    private Matrix(Map<String, String> options, PrintStream out) throws RunConfig.Invalid {
        this.extended = options.containsKey("--extended");
        this.planOnly = options.containsKey("--plan");
        this.connections = ints(options, "--connections-list", extended ? EXTENDED_CONNECTIONS : CORE_CONNECTIONS);
        this.bodies = ints(options, "--bodies-list", extended ? EXTENDED_BODIES : CORE_BODIES);
        String t = options.get("--transports");
        this.transports = t == null ? TRANSPORTS : Arrays.asList(t.split(","));
        for (String transport : transports) {
            if (!RunConfig.TRANSPORTS.contains(transport) && !BINDING_TRANSPORTS.contains(transport)) {
                throw new RunConfig.Invalid("--transports 값 '" + transport + "'을(를) 지원하지 않는다");
            }
        }
        this.warmup = options.getOrDefault("--warmup", "30s");
        this.measure = options.getOrDefault("--measure", "60s");
        this.repetitions = options.getOrDefault("--repetitions", "3");
        this.output = options.get("--output");
        this.out = out;

        RunConfig.parse(List.of("--warmup", warmup, "--measure", measure, "--repetitions", repetitions));
    }

    static Matrix parse(List<String> args, PrintStream out) throws RunConfig.Invalid {
        Set<String> known = Set.of(
                "--connections-list",
                "--bodies-list",
                "--transports",
                "--extended",
                "--plan",
                "--warmup",
                "--measure",
                "--repetitions",
                "--output");
        Set<String> flags = Set.of("--extended", "--plan");
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < args.size(); i++) {
            String name = args.get(i);
            if (!known.contains(name)) {
                throw new RunConfig.Invalid("모르는 옵션 " + name);
            }
            if (flags.contains(name)) {
                options.put(name, "true");
            } else if (i + 1 >= args.size()) {
                throw new RunConfig.Invalid(name + " 값이 없다");
            } else {
                options.put(name, args.get(++i));
            }
        }
        return new Matrix(options, out);
    }

    private static List<Integer> ints(Map<String, String> options, String name, List<Integer> fallback)
            throws RunConfig.Invalid {
        String raw = options.get(name);
        if (raw == null) {
            return fallback;
        }
        List<Integer> values = new ArrayList<>();
        for (String part : raw.split(",")) {
            try {
                int value = Integer.parseInt(part.trim());
                if (value <= 0) {
                    throw new RunConfig.Invalid(name + "의 값은 1 이상이어야 한다: " + raw);
                }
                values.add(value);
            } catch (NumberFormatException e) {
                throw new RunConfig.Invalid(name + "은(는) 쉼표로 구분한 정수여야 한다: " + raw);
            }
        }
        return values;
    }

    private record Cell(String transport, boolean tls, int connections, int bodyBytes) {}

    private List<Cell> cells() {
        List<Cell> cells = new ArrayList<>();
        for (int c : connections) {
            for (int b : bodies) {
                for (String t : transports) {
                    cells.add(new Cell(t, true, c, b));
                    if (extended) {

                        cells.add(new Cell(t, false, c, b));
                    }
                }
            }
        }
        return cells;
    }

    void run() throws Orchestrator.RunFailed, IOException, InterruptedException, RunConfig.Invalid {
        List<Cell> cells = cells();
        JsonObject result = new JsonObject();
        result.addProperty("schemaVersion", 1);
        result.addProperty("kind", "matrix");
        JsonObject metadata = Orchestrator.environmentMetadata();
        metadata.addProperty("extended", extended);
        metadata.addProperty("warmup", warmup);
        metadata.addProperty("measure", measure);
        metadata.addProperty("repetitions", Integer.parseInt(repetitions));
        result.add("metadata", metadata);

        JsonArray cellArray = new JsonArray();
        if (planOnly) {
            result.addProperty("status", "PLAN");
            for (Cell cell : cells) {
                cellArray.add(cellHeader(cell));
            }
        } else {
            Path keystoreDir = Files.createTempDirectory("strand-load-matrix");
            try {
                String keystore = cells.stream().anyMatch(Cell::tls)
                        ? LoadTls.createKeystore(keystoreDir).toString()
                        : null;
                for (Cell cell : cells) {
                    cellArray.add(runCell(cell, keystore));
                }
            } finally {
                LoadTls.deleteTree(keystoreDir);
            }
            result.addProperty("status", "OK");
        }
        result.add("cells", cellArray);

        String json = GSON.toJson(result);
        if (output == null) {
            out.println(json);
        } else {
            Path path = Paths.get(output);
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.writeString(path, json + System.lineSeparator());
            if (!planOnly) {
                out.println(TableReport.markdown(result));
            }
        }
    }

    private static JsonObject cellHeader(Cell cell) {
        JsonObject json = new JsonObject();
        json.addProperty("transport", cell.transport());
        json.addProperty("tls", cell.tls());
        json.addProperty("connections", cell.connections());
        json.addProperty("bodyBytes", cell.bodyBytes());
        return json;
    }

    private JsonObject runCell(Cell cell, String keystore)
            throws Orchestrator.RunFailed, IOException, InterruptedException, RunConfig.Invalid {
        List<String> base = new ArrayList<>(List.of(
                "--transport",
                cell.transport().startsWith("netty-") ? "netty" : cell.transport(),
                "--connections",
                String.valueOf(cell.connections()),
                "--body-bytes",
                String.valueOf(cell.bodyBytes()),
                "--warmup",
                warmup,
                "--measure",
                measure,
                "--repetitions",
                repetitions));
        if (cell.transport().startsWith("netty-")) {

            base.addAll(List.of(
                    "--loadgen-transport",
                    "netty",
                    "--binding",
                    cell.transport().equals("netty-ws") ? "websocket" : "tcp"));
        }
        if (!cell.tls()) {
            base.add("--plaintext");
        }
        JsonObject closed = new Orchestrator(RunConfig.parse(base), out, keystore).execute();
        double saturation = closed.getAsJsonObject("summary")
                .getAsJsonObject("throughputPerSecond")
                .get("median")
                .getAsDouble();

        JsonObject open = new JsonObject();
        for (String percent : new String[] {"50", "80"}) {
            List<String> args = new ArrayList<>(base);
            args.addAll(List.of(
                    "--load-model", "open", "--rate", percent + "%", "--saturation", Double.toString(saturation)));
            open.add(percent, scenario(new Orchestrator(RunConfig.parse(args), out, keystore).execute()));
        }

        JsonObject json = cellHeader(cell);
        JsonObject closedScenario = scenario(closed);
        json.add("closedLoop", closedScenario);
        json.add("openLoop", open);
        long errors = 0;
        long unknown = 0;
        List<JsonObject> scenarios = new ArrayList<>(List.of(closedScenario));
        open.entrySet().forEach(e -> scenarios.add(e.getValue().getAsJsonObject()));
        for (JsonObject scenario : scenarios) {
            for (var run : scenario.getAsJsonArray("runs")) {
                JsonObject measurement = run.getAsJsonObject().getAsJsonObject("measurement");
                errors += measurement.get("errors").getAsLong();
                unknown += measurement.get("unknown").getAsLong();
            }
        }
        json.addProperty("errors", errors);
        json.addProperty("unknown", unknown);
        return json;
    }

    private static JsonObject scenario(JsonObject result) {
        JsonObject json = new JsonObject();
        json.add("config", result.getAsJsonObject("metadata").getAsJsonObject("config"));
        json.add("runs", result.getAsJsonArray("runs"));
        json.add("summary", result.getAsJsonObject("summary"));
        return json;
    }
}
