package com.ispengya.file;

import com.ispengya.file.codec.LogRecordCodec;
import com.ispengya.file.core.MmapSequentialLog;
import com.ispengya.file.core.SequentialLog;
import com.ispengya.file.core.SequentialLogConfig;
import com.ispengya.file.model.LogRecord;
import com.ispengya.file.store.RecordStore;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * CommitLog 全局前缀恢复契约:
 *
 * <p>恢复结果必须是一条"从 0 开始、文件连续、逐帧 CRC 通过"的合法前缀。
 * 中间文件损坏/缺号时,断点之后必须整体截断并隔离,绝不允许越过空洞继续追加。</p>
 *
 * <p>小文件(256B)+ 72B 帧强制滚动:每条 3 帧,10 条记录落成 0/256/512/768 四个文件。</p>
 */
public class CommitLogPrefixRecoveryTest {

    private static final String ROOT = "./target/commitlog-prefix-test";
    private static final int FILE_SIZE = 256;
    private static final int MAX_BODY_SIZE = 64;
    private static final int RECORD_COUNT = 10;
    private static final long AFTER_FRAME_LEN = 70L;
    private static final LogRecordCodec CODEC = new LogRecordCodec();

    @Before
    public void setUp() {
        deleteRecursively(new File(ROOT));
    }

    @Test
    public void middleFileCorruptionTruncatesGlobalPrefixAndQuarantinesLaterFiles() throws Exception {
        File dir = dirFor("middle-corruption");
        long[] offsets = writeRecords(dir, RECORD_COUNT);
        assertEquals(4, numericFiles(dir).length);

        // 抹掉第二个文件首帧 magic:该文件本身及之后的数据都不再属于合法前缀
        zeroPrefix(new File(dir, "00000000000000000256"));

        SequentialLog log = open(dir);
        RecordStore<LogRecord> store = newStore(log);
        long appended;
        try {
            assertEquals(256L, log.getRecoveryTruncatedOffset());
            assertEquals(256L, log.getMaxOffset());

            for (int i = 0; i < 3; i++) {
                assertEquals(message(i), store.read(offsets[i]).getMessage());
            }
            assertReadFails(store, offsets[3]);
            assertReadFails(store, offsets[6]);

            // 截断必须落盘:后续数据文件登记进隔离清单,不再参与下次恢复
            assertEquals(4, numericFiles(dir).length);
            assertEquals(starts(512L, 768L), quarantineEntries(dir));

            appended = store.append(record("after-recovery"), 900L);
            assertEquals("截断点之后应从空洞起点继续写", 256L, appended);
            assertEquals("after-recovery", store.read(appended).getMessage());
        } finally {
            store.close();
        }

        SequentialLog reopened = open(dir);
        try {
            assertEquals("隔离后再次打开不应重复报截断", -1L, reopened.getRecoveryTruncatedOffset());
            // "after-recovery" body 34B + 帧头 36B = 70B
            assertEquals(256L + AFTER_FRAME_LEN, reopened.getMaxOffset());
        } finally {
            reopened.close();
        }
    }

    @Test
    public void missingMiddleFileTruncatesAtExpectedStart() throws Exception {
        File source = dirFor("missing-middle");
        long[] offsets = writeRecords(source, RECORD_COUNT);
        // Windows 下 mmap 未释放时无法删除旧文件,用"只复制部分文件"构造缺号目录
        File dir = copyOf(source, "missing-middle-copy", 256L);

        SequentialLog log = open(dir);
        RecordStore<LogRecord> store = newStore(log);
        try {
            assertEquals(256L, log.getRecoveryTruncatedOffset());
            assertEquals(216L, log.getMaxOffset());
            assertEquals(3, numericFiles(dir).length);
            assertEquals(starts(512L, 768L), quarantineEntries(dir));
            assertEquals(message(0), store.read(offsets[0]).getMessage());
            assertReadFails(store, offsets[3]);
            assertReadFails(store, offsets[6]);

            // 前一个文件剩余 40B 装不下 72B 帧 → 滚动并复用缺号文件起点 256
            long appended = store.append(record("after"), 900L);
            assertEquals(256L, appended);
        } finally {
            store.close();
        }
    }

    @Test
    public void nonZeroTailInSealedFileTruncatesPrefix() throws Exception {
        File dir = dirFor("dirty-sealed-tail");
        writeRecords(dir, RECORD_COUNT);

        // 第一个文件合法帧止于 216,浪费区本应全零;写入脏字节模拟残帧/外部损坏
        poke(new File(dir, "00000000000000000000"), 216L + 8, 0x5A);

        SequentialLog log = open(dir);
        RecordStore<LogRecord> store = newStore(log);
        try {
            assertEquals(216L, log.getRecoveryTruncatedOffset());
            assertEquals(216L, log.getMaxOffset());
            assertEquals(4, numericFiles(dir).length);
            assertEquals(starts(256L, 512L, 768L), quarantineEntries(dir));

            // 残尾被整段清零,新写入从滚动起点 256 开始;复用时该文件从清单移除
            long appended = store.append(record("after"), 900L);
            assertEquals(256L, appended);
            assertEquals(starts(512L, 768L), quarantineEntries(dir));
        } finally {
            store.close();
        }
    }

    @Test
    public void firstFileMissingRefusesToOpen() throws Exception {
        File source = dirFor("missing-first");
        writeRecords(source, RECORD_COUNT);
        File dir = copyOf(source, "missing-first-copy", 0L);

        try {
            open(dir);
            fail("前缀起点缺失时必须拒绝启动,不能当空库继续覆盖");
        } catch (IllegalStateException e) {
            assertTrue("异常信息应带出实际起点: " + e.getMessage(),
                    e.getMessage().contains("first data file starts at 256"));
        }
    }

    @Test
    public void recoveryHonorsMaxBodySize() throws Exception {
        File dir = dirFor("max-body");
        // 先按 1MB 上限写入一条 120 字节 body 的记录(帧 156B,仍在 256B 文件内)
        SequentialLog writable = new MmapSequentialLog(
                new SequentialLogConfig(dir.getPath(), FILE_SIZE, true, 1024 * 1024));
        RecordStore<LogRecord> writer = new RecordStore<>(writable, CODEC);
        writer.append(record(repeat('x', 120)), 1L);
        writer.close();

        // 恢复上限调成 64:该帧必须被视为不可恢复,而不是"恢复成功但读取报错"
        MmapSequentialLog strict = new MmapSequentialLog(
                new SequentialLogConfig(dir.getPath(), FILE_SIZE, true, MAX_BODY_SIZE));
        try {
            assertEquals(0L, strict.getMaxOffset());
        } finally {
            strict.close();
        }
    }

    private static SequentialLog open(File dir) {
        return new MmapSequentialLog(new SequentialLogConfig(
                dir.getPath(), FILE_SIZE, true, MAX_BODY_SIZE));
    }

    @SuppressWarnings("deprecation")
    private static RecordStore<LogRecord> newStore(SequentialLog log) {
        return new RecordStore<>(log, CODEC, null, null, MAX_BODY_SIZE);
    }

    private static long[] writeRecords(File dir, int count) {
        SequentialLog log = open(dir);
        RecordStore<LogRecord> store = newStore(log);
        long[] offsets = new long[count];
        try {
            for (int i = 0; i < count; i++) {
                offsets[i] = store.append(record(message(i)), 1000L + i);
            }
        } finally {
            store.close();
        }
        return offsets;
    }

    /**
     * 固定 16 字节 message → body 36B → 帧 72B,3 帧恰好填满 216B,留 40B 浪费区
     */
    private static String message(int i) {
        return String.format("rec-%02d-%s", i, "xxxxxxxxx");
    }

    private static LogRecord record(String message) {
        return new LogRecord(1000L, "INFO", message);
    }

    private static void assertReadFails(RecordStore<LogRecord> store, long offset) {
        try {
            LogRecord loaded = store.read(offset);
            fail("截断区不应可读,实际读到: " + loaded.getMessage());
        } catch (IllegalArgumentException expected) {
            // 文件已从队列移除/wrotePosition 之后:统一按 offset out of range 拒绝
        }
    }

    private static File dirFor(String name) {
        File dir = new File(ROOT, name);
        deleteRecursively(dir);
        return dir;
    }

    /**
     * 复制数据目录并跳过指定起始偏移的文件,构造"缺号"现场
     * (Windows 下无法删除仍被 mmap 的旧文件,复制是确定性的替代)
     */
    private static File copyOf(File source, String name, long... skipStarts) throws Exception {
        File target = new File(ROOT, name);
        deleteRecursively(target);
        if (!target.mkdirs() && !target.exists()) {
            throw new IllegalStateException("cannot create " + target);
        }
        Set<Long> skip = starts(skipStarts);
        for (File file : numericFiles(source)) {
            long start = Long.parseLong(file.getName());
            if (skip.contains(start)) {
                continue;
            }
            Files.copy(file.toPath(), new File(target, file.getName()).toPath());
        }
        return target;
    }

    private static File[] numericFiles(File dir) {
        File[] files = dir.listFiles((d, name) -> name.matches("\\d+"));
        if (files == null) {
            return new File[0];
        }
        Arrays.sort(files, Comparator.comparing(File::getName));
        return files;
    }

    private static Set<Long> quarantineEntries(File dir) throws Exception {
        Set<Long> starts = new LinkedHashSet<>();
        File file = new File(dir, ".quarantined");
        if (!file.exists()) {
            return starts;
        }
        for (String line : Files.readAllLines(file.toPath())) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty()) {
                starts.add(Long.parseLong(trimmed));
            }
        }
        return starts;
    }

    private static Set<Long> starts(long... values) {
        Set<Long> result = new LinkedHashSet<>();
        for (long value : values) {
            result.add(value);
        }
        return result;
    }

    private static void zeroPrefix(File file) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(0);
            raf.write(new byte[4]);
            raf.getChannel().force(true);
        }
    }

    private static void poke(File file, long position, int value) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(position);
            raf.write(value);
            raf.getChannel().force(true);
        }
    }

    private static String repeat(char c, int n) {
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            sb.append(c);
        }
        return sb.toString();
    }

    private static void deleteRecursively(File file) {
        if (!file.exists()) {
            return;
        }
        try {
            List<java.nio.file.Path> paths = Files.walk(file.toPath())
                    .sorted(Comparator.reverseOrder())
                    .collect(java.util.stream.Collectors.toList());
            for (java.nio.file.Path path : paths) {
                try {
                    Files.delete(path);
                } catch (Exception ignored) {
                    // Windows 下 mmap 未释放时可能删不掉;各用例目录独立,不影响本轮判定
                }
            }
        } catch (Exception ignored) {
        }
    }
}
