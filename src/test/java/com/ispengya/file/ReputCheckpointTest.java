package com.ispengya.file;

import com.ispengya.file.store.StoreCheckpoint;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.Comparator;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Checkpoint v2 双槽文件的行为契约:
 * 交替写入、load 取最新、半写槽/旧格式/缺失时安全退化。
 */
public class ReputCheckpointTest {

    private static final String ROOT = "./target/reput-checkpoint-test";

    @Before
    public void setUp() {
        deleteRecursively(new File(ROOT));
    }

    @Test
    public void saveLoadRoundTrip() throws Exception {
        StoreCheckpoint cp = new StoreCheckpoint(cpPath("round"));
        assertNull("空文件 load 应返回 null", cp.load());

        cp.save(100L, 80L, 60L, 40L);
        assertArrayEquals(new long[]{100L, 80L, 60L, 40L}, cp.load());
        assertEquals("双槽文件应为 88 字节", 88L, new File(cpPath("round")).length());

        cp.save(200L, 160L, 120L, 80L);
        assertArrayEquals(new long[]{200L, 160L, 120L, 80L}, cp.load());
    }

    @Test
    public void slotsAlternateAndKeepLatest() throws Exception {
        StoreCheckpoint cp = new StoreCheckpoint(cpPath("alt"));
        for (int i = 1; i <= 5; i++) {
            cp.save(i * 10L, i * 9L, i * 8L, i * 7L);
            assertArrayEquals("第 " + i + " 次 save 后 load 必须是最新值",
                    new long[]{i * 10L, i * 9L, i * 8L, i * 7L}, cp.load());
        }
        // 交替写入意味着最后一次覆写的是"较早"槽:两个槽都必须可解析(否则退化路径无法验证)
        // 手工打坏 seq 较大的槽 → load 应退回另一槽的旧值
        try (RandomAccessFile raf = new RandomAccessFile(new File(cpPath("alt")), "rw")) {
            long newerSlotOffset = findSlotWithMaxSeq(raf);
            raf.seek(newerSlotOffset);
            raf.write(new byte[]{0, 0, 0, 0}); // 打坏 magic
        }
        assertArrayEquals("有效槽被破坏后应退回另一槽", new long[]{40L, 36L, 32L, 28L}, cp.load());
    }

    @Test
    public void halfWrittenSlotIsIgnored() throws Exception {
        StoreCheckpoint cp = new StoreCheckpoint(cpPath("half"));
        cp.save(10L, 9L, 8L, 7L);
        cp.save(20L, 18L, 16L, 14L);
        // 模拟半写:把最新槽 CRC 覆盖区内的一个数据字节改掉(magic 不动)
        try (RandomAccessFile raf = new RandomAccessFile(new File(cpPath("half")), "rw")) {
            long newest = findSlotWithMaxSeq(raf);
            long dataPos = newest + 8; // logFlushMax 首位
            raf.seek(dataPos);
            int b = raf.read();
            raf.seek(dataPos);
            raf.write(b ^ 0xFF);
        }
        assertArrayEquals("CRC 不符槽必须被丢弃,退回上一有效槽",
                new long[]{10L, 9L, 8L, 7L}, cp.load());
    }

    @Test
    public void legacy16ByteFileTreatedAsEmpty() throws Exception {
        File f = new File(cpPath("legacy"));
        f.getParentFile().mkdirs();
        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            raf.setLength(16);
            raf.writeLong(123L);
            raf.writeLong(456L);
        }
        StoreCheckpoint cp = new StoreCheckpoint(f.getPath());
        assertNull("旧 16B 格式无 magic,应判为不可用", cp.load());
        // save 后升级为双槽并可读
        cp.save(1L, 2L, 3L, 4L);
        assertArrayEquals(new long[]{1L, 2L, 3L, 4L}, cp.load());
    }

    private static String cpPath(String caseName) {
        return ROOT + "/" + caseName + "/checkpoint.properties";
    }

    /**
     * 找出 seq 更大的槽起始偏移(要求两槽均有效)
     */
    private static long findSlotWithMaxSeq(RandomAccessFile raf) throws Exception {
        byte[] slot = new byte[44];
        int seqA;
        int seqB;
        raf.seek(0);
        raf.readFully(slot);
        seqA = readSeq(slot);
        raf.seek(44);
        raf.readFully(slot);
        seqB = readSeq(slot);
        return seqA >= seqB ? 0L : 44L;
    }

    private static int readSeq(byte[] slot) {
        return ((slot[4] & 0xFF) << 24) | ((slot[5] & 0xFF) << 16)
                | ((slot[6] & 0xFF) << 8) | (slot[7] & 0xFF);
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
