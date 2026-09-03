package com.streamflow.tools.statestore;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Doc truc tiep RocksDB state store cua Kafka Streams tren dia, khong can pipeline dang chay
 * (offline). Chi mo duoc khi khong co process nao khac dang giu RocksDB LOCK cho cung store -
 * tuc la phai dung pipeline (pod/container) truoc khi dung tool nay tren cung state.dir.
 *
 * <p>Cau truc thu muc Kafka Streams tao ra: {@code <state.dir>/<application.id>/<task.id>/rocksdb/<store.name>}.
 * Voi window/session store (vd store cua AggregateNodeBuilder), thu muc store lai chua nhieu
 * "segment" con, moi segment la 1 RocksDB instance rieng - tool tu nhan dien qua su co mat cua
 * file {@code CURRENT} o tung cap.
 */
public final class StateStoreCli {

    private static final int WINDOW_KEY_SUFFIX_SIZE = 12; // 8 byte timestamp (long) + 4 byte seqnum (int)

    private StateStoreCli() {}

    public static void main(String[] args) {
        try {
            if (args.length == 0) {
                printUsage();
                System.exit(1);
                return;
            }
            String command = args[0];
            Map<String, String> opts = parseOptions(args, 1);
            switch (command) {
                case "list" -> list(opts);
                case "dump" -> dump(opts);
                default -> {
                    System.err.println("Command khong hop le: " + command);
                    printUsage();
                    System.exit(1);
                }
            }
        } catch (IllegalArgumentException e) {
            System.err.println("Loi: " + e.getMessage());
            printUsage();
            System.exit(1);
        } catch (IOException | RocksDBException e) {
            System.err.println("Loi: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.err.println(
                """
                Doc RocksDB state store cua Kafka Streams truc tiep tu dia (offline).

                Usage:
                  list --state-dir <dir> --app-id <applicationId>
                      Liet ke cac store va task tim thay duoi <state-dir>/<app-id>.

                  dump --state-dir <dir> --app-id <applicationId> --store <storeName> [--task <taskId>] [--window-size-ms <ms>] [--limit <n>]
                      In toan bo key/value cua 1 store (gop tat ca task/partition, tru khi --task duoc chi dinh).
                      --window-size-ms: chi can khi store la window store (vd store cua Aggregate) va muon
                        tinh ca windowEnd; neu bo qua tool van tu nhan dien va in duoc windowStart.
                      --limit: gioi han so entry in ra (mac dinh khong gioi han).

                Luu y: chi chay duoc khi pipeline (Kafka Streams instance) dang KHONG chay tren cung state.dir,
                vi RocksDB chi cho 1 process mo 1 luc.
                """);
    }

    private static void list(Map<String, String> opts) throws IOException {
        Path appDir = requireAppDir(opts);
        Map<String, Set<String>> storeToTasks = new TreeMap<>();
        try (DirectoryStream<Path> taskDirs = Files.newDirectoryStream(appDir)) {
            for (Path taskDir : taskDirs) {
                if (!Files.isDirectory(taskDir)) {
                    continue;
                }
                Path rocksdbDir = taskDir.resolve("rocksdb");
                if (!Files.isDirectory(rocksdbDir)) {
                    continue;
                }
                try (DirectoryStream<Path> storeDirs = Files.newDirectoryStream(rocksdbDir)) {
                    for (Path storeDir : storeDirs) {
                        if (Files.isDirectory(storeDir)) {
                            storeToTasks
                                    .computeIfAbsent(storeDir.getFileName().toString(), k -> new TreeSet<>())
                                    .add(taskDir.getFileName().toString());
                        }
                    }
                }
            }
        }
        if (storeToTasks.isEmpty()) {
            System.out.println("Khong tim thay store nao duoi " + appDir);
            return;
        }
        storeToTasks.forEach(
                (store, tasks) -> System.out.printf("%-40s tasks=%s%n", store, tasks));
    }

    private static void dump(Map<String, String> opts) throws IOException, RocksDBException {
        Path appDir = requireAppDir(opts);
        String storeName = require(opts, "store");
        String onlyTask = opts.get("task");
        Long windowSizeMs = opts.containsKey("window-size-ms") ? Long.parseLong(opts.get("window-size-ms")) : null;
        long limit = opts.containsKey("limit") ? Long.parseLong(opts.get("limit")) : Long.MAX_VALUE;

        RocksDB.loadLibrary();
        ObjectMapper mapper = new ObjectMapper();
        long printed = 0;

        try (DirectoryStream<Path> taskDirs = Files.newDirectoryStream(appDir)) {
            for (Path taskDir : taskDirs) {
                if (printed >= limit) {
                    break;
                }
                String taskId = taskDir.getFileName().toString();
                if (onlyTask != null && !onlyTask.equals(taskId)) {
                    continue;
                }
                Path storeDir = taskDir.resolve("rocksdb").resolve(storeName);
                if (!Files.isDirectory(storeDir)) {
                    continue;
                }
                // Store khong co CURRENT o cap nay ma nam trong cac thu muc con (segment) la
                // window/session store - key cua no co 12 byte suffix (timestamp + seqnum) can bo.
                boolean windowed = !Files.exists(storeDir.resolve("CURRENT"));
                for (Path dbDir : rocksInstancesUnder(storeDir)) {
                    if (printed >= limit) {
                        break;
                    }
                    printed += dumpOneInstance(taskId, dbDir, windowed, windowSizeMs, mapper, limit - printed);
                }
            }
        }
        System.err.println("Tong so entry da in: " + printed);
    }

    private static long dumpOneInstance(
            String taskId, Path dbDir, boolean windowed, Long windowSizeMs, ObjectMapper mapper, long remaining) {
        try (Options options = new Options().setCreateIfMissing(false);
                RocksDB db = RocksDB.openReadOnly(options, dbDir.toString())) {
            long printed = 0;
            try (RocksIterator it = db.newIterator()) {
                for (it.seekToFirst(); it.isValid() && printed < remaining; it.next()) {
                    printEntry(taskId, it.key(), it.value(), windowed, windowSizeMs, mapper);
                    printed++;
                }
            }
            return printed;
        } catch (RocksDBException e) {
            System.err.println(
                    "[WARN] Khong mo duoc " + dbDir + ": " + e.getMessage()
                            + " - kiem tra pipeline co dang chay khong (RocksDB chi cho 1 process giu LOCK).");
            return 0;
        }
    }

    private static void printEntry(
            String taskId, byte[] rawKey, byte[] rawValue, boolean windowed, Long windowSizeMs, ObjectMapper mapper) {
        String valueJson = toJsonText(rawValue, mapper);
        if (windowed && rawKey.length >= WINDOW_KEY_SUFFIX_SIZE) {
            int keyLen = rawKey.length - WINDOW_KEY_SUFFIX_SIZE;
            ByteBuffer suffix = ByteBuffer.wrap(rawKey, keyLen, WINDOW_KEY_SUFFIX_SIZE);
            long windowStart = suffix.getLong();
            int seqnum = suffix.getInt();
            String key = new String(rawKey, 0, keyLen, StandardCharsets.UTF_8);
            if (windowSizeMs != null) {
                System.out.printf(
                        "[%s] key=%s windowStart=%d windowEnd=%d seq=%d value=%s%n",
                        taskId, key, windowStart, windowStart + windowSizeMs, seqnum, valueJson);
            } else {
                System.out.printf(
                        "[%s] key=%s windowStart=%d seq=%d value=%s%n", taskId, key, windowStart, seqnum, valueJson);
            }
        } else {
            String key = new String(rawKey, StandardCharsets.UTF_8);
            System.out.printf("[%s] key=%s value=%s%n", taskId, key, valueJson);
        }
    }

    private static List<Path> rocksInstancesUnder(Path storeDir) throws IOException {
        if (Files.exists(storeDir.resolve("CURRENT"))) {
            return List.of(storeDir);
        }
        List<Path> segments = new ArrayList<>();
        try (DirectoryStream<Path> children = Files.newDirectoryStream(storeDir)) {
            for (Path child : children) {
                if (Files.isDirectory(child) && Files.exists(child.resolve("CURRENT"))) {
                    segments.add(child);
                }
            }
        }
        segments.sort(Comparator.comparing(p -> p.getFileName().toString()));
        return segments;
    }

    private static String toJsonText(byte[] rawValue, ObjectMapper mapper) {
        if (rawValue == null) {
            return "null";
        }
        try {
            return mapper.writeValueAsString(mapper.readTree(rawValue));
        } catch (IOException e) {
            return "<non-JSON, " + rawValue.length + " bytes>";
        }
    }

    private static Path requireAppDir(Map<String, String> opts) {
        String stateDir = require(opts, "state-dir");
        String appId = require(opts, "app-id");
        Path appDir = Paths.get(stateDir, appId);
        if (!Files.isDirectory(appDir)) {
            throw new IllegalArgumentException("Khong tim thay thu muc state cho application.id nay: " + appDir);
        }
        return appDir;
    }

    private static String require(Map<String, String> opts, String key) {
        String value = opts.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Thieu tham so bat buoc --" + key);
        }
        return value;
    }

    private static Map<String, String> parseOptions(String[] args, int from) {
        Map<String, String> opts = new HashMap<>();
        for (int i = from; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException("Tham so khong hop le: " + arg);
            }
            String key = arg.substring(2);
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("Thieu gia tri cho --" + key);
            }
            opts.put(key, args[++i]);
        }
        return opts;
    }
}
