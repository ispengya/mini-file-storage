package com.ispengya.file;

import com.ispengya.file.index.KeyIndexFile;
import com.ispengya.file.index.SimpleConsumeQueue;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * 索引类观察/截断原语(Reput 的地基):count、cqCursor 自时钟、
 * truncateBeyond 交叉校验截断、跨文件截断后的原位重写。
 */
public class IndexCursorTest {

    private static final String ROOT = "./target/index-cursor-test";

    /**
     * 64 字节会向下对齐为 60 = 3 条/文件,便于制造多文件场景
     */
    private static final int CQ_FILE_SIZE = 64;

    @Before
    public void setUp() {
        deleteRecursively(new File(ROOT));
    }

    private SimpleConsumeQueue newQueue(String name) throws Exception {
        return new SimpleConsumeQueue(dir(name), CQ_FILE_SIZE, true);
    }

    @Test
    public void cursorTracksLastEntry() throws Exception {
        SimpleConsumeQueue cq = newQueue("cursor");
        assertEquals(0L, cq.cqCursor());
        for (int i = 0; i < 5; i++) {
            cq.append(i * 100L, 50, i);
        }
        assertEquals(5L, cq.count());
        assertEquals(450L, cq.cqCursor()); // 最后一条 off=400 + size=50
        cq.close();
    }

    @Test
    public void truncateBeyondDropsLyingTail() throws Exception {
        SimpleConsumeQueue cq = newQueue("truncate");
        for (int i = 0; i < 5; i++) {
            cq.append(i * 100L, 50, i);
        }
        // 数据真相只到 349:末两条(350/450 结尾)是"撒谎指针"
        long dropped = cq.truncateBeyond(349L);
        assertEquals(2L, dropped);
        assertEquals(3L, cq.count());
        assertEquals(250L, cq.cqCursor());
        assertNull(cq.get(3));
        assertNull(cq.get(4));
        assertEquals(200L, cq.get(2).getPhysicalOffset());

        // 截断后原位重写:新条目成为第 4 条(索引 3)
        cq.append(350L, 50, 99L);
        assertEquals(4L, cq.count());
        assertEquals(400L, cq.cqCursor());
        assertEquals(350L, cq.get(3).getPhysicalOffset());
        assertEquals(99L, cq.get(3).getTagCode());
        cq.close();
    }

    @Test
    public void truncateAcrossFilesPopsWholeFiles() throws Exception {
        SimpleConsumeQueue cq = newQueue("multi-file");
        for (int i = 0; i < 5; i++) {
            cq.append(i * 100L, 50, i); // 3 条占满第一文件,后 2 条在第二文件
        }
        long dropped = cq.truncateBeyond(0L);
        assertEquals(5L, dropped);
        assertEquals(0L, cq.count());
        assertEquals(0L, cq.cqCursor());

        // 全截断后从头追加:覆盖旧文件字节,读不到任何残留
        cq.append(0L, 40, 7L);
        assertEquals(1L, cq.count());
        assertEquals(40L, cq.cqCursor());
        assertEquals(7L, cq.get(0).getTagCode());
        assertNull(cq.get(1));
        cq.close();
    }

    @Test
    public void keyIndexExposesCount() throws Exception {
        KeyIndexFile keyIndex = new KeyIndexFile(dir("key-count"), 64 * 1024);
        assertEquals(0L, keyIndex.count());
        keyIndex.put(11L, 0L);
        keyIndex.put(22L, 100L);
        keyIndex.put(11L, 200L);
        assertEquals(3L, keyIndex.count());
        keyIndex.close();
    }

    private static String dir(String name) {
        File file = new File(ROOT, name);
        file.deleteOnExit();
        return file.getPath();
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
