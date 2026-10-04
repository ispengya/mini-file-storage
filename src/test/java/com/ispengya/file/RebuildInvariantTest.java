package com.ispengya.file;

import com.ispengya.file.api.FileStore;
import com.ispengya.file.api.MiniFileStore;
import com.ispengya.file.api.StoreConfig;
import com.ispengya.file.codec.LogRecordCodec;
import com.ispengya.file.model.LogRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * 唯一真相源的契约测试:ConsumeQueue 与 KeyIndex 是"可弃的派生视图"。
 *
 * <p>物理删除两份索引目录 + checkpoint 后重开门面,全部索引查询必须与删除前
 * 逐条一致——由启动 catch-up 回放 CommitLog 自动完成。这就是 Reput 架构
 * 区别于"同步双写"的根本承诺:索引丢了不是事故,是重建的触发条件。</p>
 */
public class RebuildInvariantTest {

    private static final String ROOT = "./target/rebuild-invariant-test";
    private static final LogRecordCodec CODEC = new LogRecordCodec();

    @Before
    public void setUp() {
        deleteRecursively(new File(ROOT));
    }

    @Test
    public void rebuildAllIndexesFromCommitLogAfterDirsDeleted() throws Exception {
        StoreConfig config = StoreConfig.builder(new File(ROOT, "store").getPath()).build();

        // 第一段:写入混合负载。
        // 注意 queryByTagRange 的前提是 tagCode 单调——负载设计成"显式递增 tag 在前,
        // 挂钟 tag(put-with-key)在后",两组之间仍满足单调性
        List<String> expectedLogical = new ArrayList<>();
        FileStore<LogRecord> store = new MiniFileStore<>(config, CODEC);
        long baseTag = 1000L;
        for (int i = 0; i < 20; i++) {
            String msg = "rec-" + i;
            expectedLogical.add(msg);
            store.put(record(baseTag + i, msg), baseTag + i); // 显式 tag,无 key
        }
        for (int j = 0; j < 5; j++) {
            String msg = "keyed-" + j;
            expectedLogical.add(msg);
            store.put(record(System.currentTimeMillis(), msg), j % 2 == 0 ? "k-a" : "k-b");
        }
        List<String> expectedKA = messagesOf(store.getByKey("k-a"));
        List<String> expectedKB = messagesOf(store.getByKey("k-b"));
        List<String> expectedRange = messagesOf(store.queryByTagRange(baseTag + 5, baseTag + 12));
        assertEquals(3, expectedKA.size());
        assertEquals(2, expectedKB.size());
        assertEquals(8, expectedRange.size());
        store.flush();
        store.close();

        // 删除派生视图(索引目录 + checkpoint),只留 CommitLog。
        // Windows 下 mmap 悬挂到 Cleaner 运行,提示 GC 后删除;跨进程重启则天然无此问题
        unmapGracefully();
        deleteRecursively(new File(config.consumeQueuePath()));
        deleteRecursively(new File(config.keyIndexPath()));
        new File(config.checkpointPath()).delete();
        assertFalse("CQ 目录必须已删除", new File(config.consumeQueuePath()).exists());
        assertFalse("KeyIndex 目录必须已删除", new File(config.keyIndexPath()).exists());

        // 第二段:重开 = 启动 catch-up 从日志全量重建
        FileStore<LogRecord> rebuilt = new MiniFileStore<>(config, CODEC);
        try {
            for (int i = 0; i < expectedLogical.size(); i++) {
                LogRecord loaded = rebuilt.getByLogicalIndex(i);
                assertEquals("重建后逻辑位点 " + i, expectedLogical.get(i), loaded.getMessage());
            }
            assertEquals(expectedKA, messagesOf(rebuilt.getByKey("k-a")));
            assertEquals(expectedKB, messagesOf(rebuilt.getByKey("k-b")));
            assertEquals(expectedRange, messagesOf(rebuilt.queryByTagRange(baseTag + 5, baseTag + 12)));

            // 重建后可以继续正常写
            long next = rebuilt.put(record(baseTag + 20, "rec-20"), "k-a");
            assertTrue(rebuilt.awaitIndexed(next, 3000));
            assertEquals("rec-20", rebuilt.getByLogicalIndex(expectedLogical.size()).getMessage());
            assertEquals(4, rebuilt.getByKey("k-a").size());
        } finally {
            rebuilt.close();
        }
    }

    private static LogRecord record(long ts, String msg) {
        return new LogRecord(ts, "INFO", msg);
    }

    private static List<String> messagesOf(List<LogRecord> records) {
        List<String> out = new ArrayList<>(records.size());
        for (LogRecord r : records) {
            out.add(r.getMessage());
        }
        return out;
    }

    private static void unmapGracefully() throws Exception {
        for (int i = 0; i < 3; i++) {
            System.gc();
            Thread.sleep(120);
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
