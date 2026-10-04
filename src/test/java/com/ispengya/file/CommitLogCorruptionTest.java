package com.ispengya.file;

import com.ispengya.file.api.FileStore;
import com.ispengya.file.api.MiniFileStore;
import com.ispengya.file.api.StoreConfig;
import com.ispengya.file.api.StoreCorruptedException;
import com.ispengya.file.api.StoreLockHeldException;
import com.ispengya.file.codec.LogRecordCodec;
import com.ispengya.file.core.MmapSequentialLog;
import com.ispengya.file.core.SequentialLog;
import com.ispengya.file.core.SequentialLogConfig;
import com.ispengya.file.core.RecordFrame;
import com.ispengya.file.model.LogRecord;
import com.ispengya.file.store.RecordStore;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.Comparator;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * CommitLog 损坏安全性的故障注入测试,逐一对应四个漏洞:
 *
 * <ol>
 *   <li>撕裂帧(magic/结构完好、body 被改脏)必须让恢复扫描<b>停在其之前</b>(v1 会静默越过)</li>
 *   <li>读路径 CRC 校验:内容脏了必须抛 {@link StoreCorruptedException},绝不返回错误对象</li>
 *   <li>同一目录双开必须被进程锁拒绝({@link StoreLockHeldException})</li>
 *   <li>恢复后脏尾被置零清除,不再残留在盘上</li>
 * </ol>
 *
 * <p>场景 1/2/4 直接操作数据文件(RandomAccessFile 改脏/写脏尾)模拟"断电撕裂"与"盘上老化"。</p>
 */
public class CommitLogCorruptionTest {

    private static final String TEST_ROOT = "./target/mini-file-corruption-test";
    private static final int FILE_SIZE = 1024 * 1024;
    private static final String DATA_FILE_NAME = "00000000000000000000";

    @Test
    public void tornFrameStopsRecoveryBeforeIt() throws Exception {
        File dataDir = dirFor("torn-stop");
        LogRecordCodec codec = new LogRecordCodec();

        SequentialLog log1 = openLog(dataDir);
        RecordStore<LogRecord> store1 = new RecordStore<>(log1, codec);
        store1.append(record("intact-1"), 100L);
        long tornOffset = store1.append(record("torn-2"), 200L);
        store1.close();
        log1.close();

        // 模拟断电页撕裂:magic/bodyLen 所在前部不动,只改脏 body 一个字节
        poke(new File(dataDir, DATA_FILE_NAME), tornOffset + RecordFrame.HEADER_SIZE + 1, 0xFF);

        SequentialLog log2 = openLog(dataDir);
        // 核心断言:恢复必须停在撕裂帧起点(=上一条完整帧结束),而不是越过它
        assertEquals(tornOffset, log2.getMaxOffset());

        // 撕裂帧被脏尾清除覆盖后,新记录恰好复用该位置
        RecordStore<LogRecord> store2 = new RecordStore<>(log2, codec);
        long reused = store2.append(record("fresh-3"), 300L);
        assertEquals(tornOffset, reused);
        assertEquals("fresh-3", store2.read(reused).getMessage());
        store2.close();
        log2.close();
    }

    @Test
    public void dirtyBodyReadThrowsCorruption() throws Exception {
        File dataDir = dirFor("read-check");
        LogRecordCodec codec = new LogRecordCodec();

        SequentialLog log = openLog(dataDir);
        RecordStore<LogRecord> store = new RecordStore<>(log, codec);
        long offset = store.append(record("will-be-rotted"), 100L);
        store.flush();

        // 存储实例开着的情况下改脏 body(模拟盘上静默损坏被读到)
        poke(new File(dataDir, DATA_FILE_NAME), offset + RecordFrame.HEADER_SIZE + 2, 0xAA);

        try {
            store.read(offset);
            fail("CRC 不匹配的读取必须抛 StoreCorruptedException");
        } catch (StoreCorruptedException e) {
            assertEquals(offset, e.getPhysicalOffset());
        }
        store.close();
        log.close();
    }

    @Test
    public void doubleOpenIsRejectedByStoreLock() throws Exception {
        File dir = dirFor("lock");
        StoreConfig config = StoreConfig.builder(dir.getPath()).build();
        FileStore<LogRecord> first = new MiniFileStore<>(config, new LogRecordCodec());
        try {
            first.put(record("owner-1"));

            try {
                new MiniFileStore<>(config, new LogRecordCodec());
                fail("同一 baseDir 双开必须抛 StoreLockHeldException");
            } catch (StoreLockHeldException expected) {
                // 锁文件路径应出现在异常信息内,便于运维定位
                assertEquals(dir.getPath() + "/.store.lock", expected.getLockFilePath());
            }
        } finally {
            first.close();
        }

        // 关闭后锁释放,重新打开可读旧数据
        FileStore<LogRecord> second = new MiniFileStore<>(config, new LogRecordCodec());
        try {
            assertEquals("owner-1", second.getByLogicalIndex(0).getMessage());
        } finally {
            second.close();
        }
    }

    @Test
    public void dirtyTailIsZeroedOnRecovery() throws Exception {
        File dataDir = dirFor("tail-clear");
        LogRecordCodec codec = new LogRecordCodec();

        SequentialLog log1 = openLog(dataDir);
        RecordStore<LogRecord> store1 = new RecordStore<>(log1, codec);
        long offset = store1.append(record("clean-1"), 100L);
        int frameEnd = (int) (offset + RecordFrame.HEADER_SIZE + codec.encode(record("clean-1")).length);
        store1.flush();
        store1.close();
        log1.close();

        // 在合法帧尾之后写入非零垃圾(模拟崩溃时半写入)
        poke(new File(dataDir, DATA_FILE_NAME), frameEnd, 0x4A, 0x55, 0x4E, 0x4B);

        SequentialLog log2 = openLog(dataDir);
        assertEquals(frameEnd, log2.getMaxOffset());
        log2.close();

        // 脏尾必须已被置零并落盘
        byte[] tail = new byte[16];
        try (RandomAccessFile raf = new RandomAccessFile(new File(dataDir, DATA_FILE_NAME), "r")) {
            raf.seek(frameEnd);
            raf.readFully(tail);
        }
        assertArrayEquals(new byte[16], tail);
    }

    private static SequentialLog openLog(File dataDir) {
        return new MmapSequentialLog(new SequentialLogConfig(dataDir.getPath(), FILE_SIZE, true));
    }

    private static LogRecord record(String message) {
        return new LogRecord(1000L, "INFO", message);
    }

    /**
     * 通过独立文件句柄改脏指定字节(页缓存共享,mmap 视图可见)
     */
    private static void poke(File file, long position, int... bytes) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(position);
            for (int b : bytes) {
                raf.write(b);
            }
            raf.getChannel().force(true);
        }
    }

    private static File dirFor(String caseName) {
        File dir = new File(TEST_ROOT, caseName);
        deleteRecursively(dir);
        dir.deleteOnExit();
        return dir;
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
                            // 同 JVM 内 mmap 未释放会删不掉;用例目录互不相同,不影响判定
                        }
                    });
        } catch (Exception ignored) {
        }
    }
}
