package com.ispengya.file;

import com.ispengya.file.codec.LogRecordCodec;
import com.ispengya.file.core.MmapSequentialLog;
import com.ispengya.file.core.RecordFrame;
import com.ispengya.file.core.SequentialLog;
import com.ispengya.file.core.SequentialLogConfig;
import com.ispengya.file.index.KeyIndexFile;
import com.ispengya.file.index.SimpleConsumeQueue;
import com.ispengya.file.model.LogRecord;
import com.ispengya.file.store.RecordStore;
import com.ispengya.file.store.ReputService;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * ReputService 行为契约:同步回放、awaitCovered 谓词、后台线程、损坏停点。
 * 用"纯日志"RecordStore(2 参构造,不内联写索引)喂数据,索引只应经 Reput 产生。
 */
public class ReputServiceTest {

    private static final String ROOT = "./target/reput-service-test";
    private static final int FILE_SIZE = 1024 * 1024;

    private MmapSequentialLog log;
    private SimpleConsumeQueue cq;
    private KeyIndexFile keyIndex;
    private RecordStore<LogRecord> store;
    private ReputService reput;
    private final LogRecordCodec codec = new LogRecordCodec();

    @Before
    public void setUp() throws Exception {
        deleteRecursively(new File(ROOT));
    }

    private void wire(String caseName) throws Exception {
        File base = new File(ROOT, caseName);
        File dataDir = new File(base, "data");
        File cqDir = new File(base, "cq");
        File keyDir = new File(base, "key");
        dataDir.mkdirs();
        cqDir.mkdirs();
        keyDir.mkdirs();
        log = new MmapSequentialLog(new SequentialLogConfig(dataDir.getPath(), FILE_SIZE, false));
        cq = new SimpleConsumeQueue(cqDir.getPath(), 200 * 1024, true);
        keyIndex = new KeyIndexFile(keyDir.getPath(), 64 * 1024, true);
        store = new RecordStore<>(log, codec); // 纯日志模式:append 不碰索引
        reput = new ReputService(log, cq, keyIndex, 1024 * 1024);
        reput.initCursors(cq.cqCursor(), -1L);
    }

    private int frameLen(String message) {
        return RecordFrame.HEADER_SIZE + codec.encode(new LogRecord(0L, "INFO", message)).length;
    }

    @Test
    public void replayBuildsBothIndexesFromFrames() throws Exception {
        wire("replay");
        long off0 = store.append(record("a"), 1L);
        long off1 = store.append(record("b"), 2L, 42L); // 带 keyHash 的帧
        long max = log.getMaxOffset();

        assertEquals(max, reput.replayTo(max));

        assertEquals(2L, cq.count());
        assertEquals(off0, cq.get(0).getPhysicalOffset());
        assertEquals(frameLen("a"), cq.get(0).getSize());
        assertEquals(1L, cq.get(0).getTagCode());
        assertEquals(off1, cq.get(1).getPhysicalOffset());
        assertEquals(2L, cq.get(1).getTagCode());
        assertEquals(1L, keyIndex.count()); // keyHash=42 的帧被重建
        assertEquals(1L, keyIndex.get(42L).size());
        assertEquals(max, reput.getCqCursor());
        closeAll();
    }

    @Test
    public void awaitCoveredUsesStrictlyPastSemantics() throws Exception {
        wire("await");
        long off0 = store.append(record("a"), 1L);
        reput.signal();

        // 回放前:cursor==0 <= off0(0),必须判"未覆盖"
        assertFalse(reput.awaitCovered(off0, 30));

        reput.replayTo(log.getMaxOffset());
        assertTrue("整帧完成后 cursor 严格越过起始位点", reput.awaitCovered(off0, 30));
        assertTrue(reput.awaitCovered(0L, 30));
        closeAll();
    }

    @Test
    public void workerThreadTracksAppends() throws Exception {
        wire("worker");
        reput.startWorker();
        try {
            long off0 = store.append(record("w1"), 10L);
            reput.signal();
            assertTrue("后台线程应追平", reput.awaitCovered(off0, 3000));
            long off1 = store.append(record("w2"), 20L, 7L);
            reput.signal();
            assertTrue(reput.awaitCovered(off1, 3000));
            assertEquals(2L, cq.count());
            assertEquals(1L, keyIndex.count());
        } finally {
            reput.stopAndDrain(2000);
        }
        closeAll();
    }

    @Test
    public void corruptFrameStopsReplayAtBoundary() throws Exception {
        wire("corrupt");
        long off0 = store.append(record("ok"), 1L);
        long off1 = store.append(record("bad"), 2L);
        store.flush();
        // 打脏第二帧的 body 一个字节(CRC 必炸)
        poke(new File(ROOT, "corrupt/data/00000000000000000000"),
                off1 + RecordFrame.HEADER_SIZE + 1, 0xFF);

        long reached = reput.replayTo(log.getMaxOffset());
        assertEquals("回放必须停在坏帧之前", off1, reached);
        assertEquals(off1, reput.getCqCursor());
        assertEquals(1L, cq.count());
        assertNotNull("错误必须可见", reput.getLastError());
        assertFalse("await 在错误态不再等待成功", reput.awaitCovered(off1, 30));
        closeAll();
    }

    private static LogRecord record(String message) {
        return new LogRecord(System.currentTimeMillis(), "INFO", message);
    }

    private void closeAll() {
        keyIndex.close();
        cq.close();
        store.close();
    }

    private static void poke(File file, long position, int value) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(position);
            int b = raf.read();
            raf.seek(position);
            raf.write(b ^ value);
            raf.getChannel().force(true);
        }
    }

    private static void deleteRecursively(File file) {
        if (!file.exists()) {
            return;
        }
        try {
            Files.walk(file.toPath())
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (Exception ignored) {
                        }
                    });
        } catch (Exception ignored) {
        }
    }
}
