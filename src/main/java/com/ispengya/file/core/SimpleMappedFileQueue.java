package com.ispengya.file.core;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 简化版映射文件队列
 * 管理同一目录下的一组顺序文件，负责文件滚动与统一刷盘。
 *
 * <p>内置 CommitLog/CQ/KeyIndex 的格式特定恢复之外,{@link #recoverWith}
 * 允许自定义索引文件复用同一套分段管理与恢复流程。</p>
 */
public class SimpleMappedFileQueue {
    /**
     * 隔离清单文件名(非纯数字,不参与数据文件枚举)
     */
    private static final String QUARANTINE_FILE = ".quarantined";

    /**
     * 存储目录
     */
    private final String storePath;
    /**
     * 单个文件大小
     */
    private final int fileSize;
    /**
     * 已创建的映射文件列表，按起始偏移有序
     */
    private final List<SimpleMappedFile> mappedFiles = new ArrayList<>();

    /**
     * 本次恢复发生全局前缀截断的终点物理位点;-1 表示日志是完整连续前缀
     */
    private long recoveryTruncatedOffset = -1L;

    public SimpleMappedFileQueue(String storePath, int fileSize) {
        this.storePath = storePath;
        this.fileSize = fileSize;
        SimpleMappedFile.ensureDirOK(storePath);
    }

    /**
     * 自定义分段格式的恢复扫描器
     */
    @FunctionalInterface
    public interface SegmentScanner {
        /**
         * 扫描单个分段文件
         *
         * @param file     当前数据文件
         * @param fileSize 预分配文件大小
         * @param mapped   只读映射视图(position=0, limit=fileSize)
         * @return 有效写入位置(字节数,必须在 [0, fileSize] 内)
         */
        int scan(File file, int fileSize, ByteBuffer mapped);
    }

    /**
     * 通用分段恢复:分段文件管理/滚动/刷盘复用 {@link SimpleMappedFileQueue},
     * 每个文件的有效区由调用方通过 {@link SegmentScanner} 定义。
     *
     * <p>适用于自定义 ConsumeQueue / IndexFile 等派生索引文件。它只做逐文件恢复,
     * 不做 CommitLog 的全局前缀/隔离语义——派生索引损坏时应由索引自己决定
     * "从 0 全量重建"还是保留前缀。</p>
     */
    public static SimpleMappedFileQueue recoverWith(String storePath, int fileSize,
                                                     SegmentScanner scanner) throws IOException {
        if (scanner == null) {
            throw new IllegalArgumentException("scanner must not be null");
        }
        SimpleMappedFileQueue queue = new SimpleMappedFileQueue(storePath, fileSize);
        File[] files = listNumericFiles(storePath, Collections.emptySet());
        for (File file : files) {
            long start = Long.parseLong(file.getName());
            SimpleMappedFile mappedFile = new SimpleMappedFile(file.getPath(), fileSize, start);
            int wrote;
            try {
                wrote = scanner.scan(file, fileSize, mappedFile.mappedView());
            } catch (RuntimeException e) {
                mappedFile.close();
                throw new IOException("scan index segment failed: " + file, e);
            }
            if (wrote < 0 || wrote > fileSize) {
                mappedFile.close();
                throw new IOException("scanner returned invalid position " + wrote + " for " + file);
            }
            mappedFile.setWrotePosition(wrote);
            mappedFile.setFlushedPosition(wrote);
            // 清掉有效区之后的残片,防止下次恢复把旧 entry 重新采纳
            mappedFile.zeroTailFrom(wrote);
            if (wrote > 0 || queue.mappedFiles.isEmpty()) {
                queue.mappedFiles.add(mappedFile);
            } else {
                mappedFile.close();
            }
        }
        return queue;
    }

    /**
     * 兼容旧调用的入口:bodyLen 不做额外上限约束
     */
    public static SimpleMappedFileQueue recoverForRecordStore(String storePath, int fileSize) throws IOException {
        return recoverForRecordStore(storePath, fileSize, Integer.MAX_VALUE);
    }

    /**
     * 全局前缀恢复 CommitLog。
     *
     * <p>恢复结果必须是一条"从 0 开始、文件连续、逐帧 CRC 通过"的合法前缀,
     * 而不是每个文件各自为政:</p>
     * <ol>
     *   <li>首个文件必须从 0 开始、文件起始偏移连续,否则说明前缀起点已丢失,
     *       直接拒绝启动(避免把现存数据当空库覆盖);</li>
     *   <li>非末文件必须是完整封存文件:至少一帧,且停点之后全零(正常滚动留下的
     *       浪费区本来就是预分配零字节);出现残帧即视为损坏;</li>
     *   <li>一旦发现损坏/缺号,在当前断点做<b>全局截断</b>:保留断点前的合法前缀,
     *       整段清零残尾,并把之后的文件登记进 {@code .quarantined} 清单永久隔离
     *       (同时抹掉其首帧 magic,清单丢失时也不会被重新采纳);</li>
     *   <li>只有末文件允许存在撕裂残尾,清零后从断点继续追加;</li>
     *   <li>日志增长到隔离文件起点时,先整文件清零再复用,并从清单移除。</li>
     * </ol>
     *
     * <p>隔离用清单而不是 rename:Windows 上 mmap 未释放时数据文件无法改名/删除,
     * 清单 + 清零在两个平台语义一致。截断终点可通过
     * {@link #getRecoveryTruncatedOffset()} 观测。</p>
     *
     * @param maxBodySize 恢复扫描允许的最大 body 长度,必须与读路径一致
     */
    public static SimpleMappedFileQueue recoverForRecordStore(String storePath, int fileSize, int maxBodySize)
            throws IOException {
        SimpleMappedFileQueue queue = new SimpleMappedFileQueue(storePath, fileSize);
        Set<Long> quarantined = loadQuarantine(storePath);
        File[] files = listNumericFiles(storePath, quarantined);
        if (files.length == 0) {
            return queue;
        }

        long expectedStart = 0L;
        for (int i = 0; i < files.length; i++) {
            File file = files[i];
            long start = Long.parseLong(file.getName());
            boolean last = i == files.length - 1;

            if (start != expectedStart) {
                if (queue.mappedFiles.isEmpty()) {
                    throw new IllegalStateException("commitlog prefix is broken: first data file starts at "
                            + start + " (expected 0) in " + storePath);
                }
                // 文件缺号/多出非法起点:合法前缀到此为止,后面全部隔离
                queue.recoveryTruncatedOffset = expectedStart;
                quarantineFrom(storePath, files, i);
                return queue;
            }

            SimpleMappedFile mappedFile = new SimpleMappedFile(file.getPath(), fileSize, start);
            int wrote = mappedFile.recoverRecordStoreWrotePosition(maxBodySize);
            queue.mappedFiles.add(mappedFile);

            if (last) {
                // 末文件:允许断电撕裂的残尾,整段清零后从断点继续写
                mappedFile.zeroTailFrom(wrote);
                return queue;
            }

            // 非末文件:必须是完整封存文件。空文件或非零残尾都说明前缀在此断裂
            if (wrote == 0 || !mappedFile.isTailZero(wrote)) {
                mappedFile.zeroTailFrom(wrote);
                queue.recoveryTruncatedOffset = start + wrote;
                quarantineFrom(storePath, files, i + 1);
                return queue;
            }

            expectedStart = start + fileSize;
        }
        return queue;
    }

    /**
     * 列出目录下文件名可解析为物理起始偏移、且未被隔离清单排除的文件,按偏移排序
     */
    private static File[] listNumericFiles(String storePath, Set<Long> quarantined) {
        File[] files = new File(storePath).listFiles();
        if (files == null || files.length == 0) {
            return new File[0];
        }
        List<File> numeric = new ArrayList<>();
        for (File file : files) {
            if (!file.isFile()) {
                continue;
            }
            long start;
            try {
                start = Long.parseLong(file.getName());
            } catch (NumberFormatException e) {
                continue;
            }
            if (quarantined.contains(start)) {
                continue;
            }
            numeric.add(file);
        }
        numeric.sort(Comparator.comparing(File::getName));
        return numeric.toArray(new File[0]);
    }

    /**
     * 把从 {@code from} 起的数据文件登记进隔离清单,并抹掉首帧 magic。
     *
     * <p>先落清单再抹 magic:正确性依赖清单;magic 清零只是清单丢失时的安全网——
     * 残文件会因"非末文件为空"被再次截断,不会被静默采纳。</p>
     */
    private static void quarantineFrom(String storePath, File[] files, int from) throws IOException {
        if (from >= files.length) {
            return;
        }
        Set<Long> merged = loadQuarantine(storePath);
        for (int i = from; i < files.length; i++) {
            merged.add(Long.parseLong(files[i].getName()));
        }
        saveQuarantine(storePath, merged);
        for (int i = from; i < files.length; i++) {
            truncateMagic(files[i]);
        }
    }

    /**
     * 读取隔离清单;清单损坏时拒绝恢复,避免带着未知隔离状态继续写
     */
    private static Set<Long> loadQuarantine(String storePath) {
        File file = new File(storePath, QUARANTINE_FILE);
        Set<Long> starts = new LinkedHashSet<>();
        if (!file.exists()) {
            return starts;
        }
        try {
            for (String line : Files.readAllLines(file.toPath(), StandardCharsets.UTF_8)) {
                String trimmed = line.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                try {
                    starts.add(Long.parseLong(trimmed));
                } catch (NumberFormatException e) {
                    throw new IllegalStateException("invalid quarantine entry '" + trimmed
                            + "' in " + file.getPath(), e);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed to read commitlog quarantine file: " + file.getPath(), e);
        }
        return starts;
    }

    /**
     * 原子写入隔离清单;清单为空时直接删除
     */
    private static void saveQuarantine(String storePath, Set<Long> starts) throws IOException {
        File file = new File(storePath, QUARANTINE_FILE);
        if (starts.isEmpty()) {
            Files.deleteIfExists(file.toPath());
            return;
        }
        StringBuilder content = new StringBuilder();
        for (Long start : starts) {
            content.append(start).append('\n');
        }
        Path target = file.toPath();
        Path tmp = Files.createTempFile(target.getParent(), ".quarantine", ".tmp");
        boolean moved = false;
        try {
            Files.write(tmp, content.toString().getBytes(StandardCharsets.UTF_8));
            try {
                Files.move(tmp, target,
                        StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
            moved = true;
        } finally {
            if (!moved) {
                Files.deleteIfExists(tmp);
            }
        }
    }

    /**
     * 抹掉文件首帧 magic,使该文件在清单丢失时也无法被当作合法尾部
     */
    private static void truncateMagic(File file) {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(0);
            raf.write(new byte[4]);
            raf.getChannel().force(true);
        } catch (IOException ignored) {
            // 清单已落盘,清零失败不影响隔离正确性
        }
    }

    /**
     * 复用隔离文件前:整文件清零,移除清单条目。
     * 先清零后移除——中途崩溃最多重复一次清零,不会把残片当新数据。
     */
    private void reclaimQuarantined(long startOffset) throws IOException {
        Set<Long> quarantined = loadQuarantine(storePath);
        if (!quarantined.remove(startOffset)) {
            return;
        }
        File file = new File(storePath, formatFileName(startOffset));
        if (file.exists()) {
            zeroFile(file);
        }
        saveQuarantine(storePath, quarantined);
    }

    /**
     * 覆盖写零整个数据文件(等价于清空);Windows 下 mmap 未释放时仍可写,不需要删除
     */
    private void zeroFile(File file) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            if (raf.length() != fileSize) {
                raf.setLength(fileSize);
            }
            byte[] zeros = new byte[8192];
            long written = 0;
            while (written < fileSize) {
                int len = (int) Math.min(zeros.length, fileSize - written);
                raf.write(zeros, 0, len);
                written += len;
            }
            raf.getChannel().force(true);
        }
    }

    public static SimpleMappedFileQueue recoverForConsumeQueue(String storePath, int fileSize) throws IOException {
        SimpleMappedFileQueue queue = new SimpleMappedFileQueue(storePath, fileSize);
        File dir = new File(storePath);
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            return queue;
        }
        Arrays.sort(files, Comparator.comparing(File::getName));
        for (File file : files) {
            if (!file.isFile()) {
                continue;
            }
            long startOffset;
            try {
                startOffset = Long.parseLong(file.getName());
            } catch (NumberFormatException e) {
                continue;
            }
            SimpleMappedFile mappedFile = new SimpleMappedFile(file.getPath(), fileSize, startOffset);
            int wrote = mappedFile.recoverConsumeQueueWrotePosition();
            if (wrote > 0 || queue.mappedFiles.isEmpty()) {
                queue.mappedFiles.add(mappedFile);
            } else {
                mappedFile.close();
            }
        }
        return queue;
    }

    /**
     * 按 16 字节单元扫描恢复 KeyIndex 文件队列
     * 与 recoverForConsumeQueue 同构,用于防止重启后从文件头覆盖旧索引
     *
     * @param storePath 索引目录
     * @param fileSize  单文件大小
     * @return 恢复写入位置后的文件队列
     */
    public static SimpleMappedFileQueue recoverForKeyIndex(String storePath, int fileSize) throws IOException {
        SimpleMappedFileQueue queue = new SimpleMappedFileQueue(storePath, fileSize);
        File dir = new File(storePath);
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            return queue;
        }
        Arrays.sort(files, Comparator.comparing(File::getName));
        for (File file : files) {
            if (!file.isFile()) {
                continue;
            }
            long startOffset;
            try {
                startOffset = Long.parseLong(file.getName());
            } catch (NumberFormatException e) {
                continue;
            }
            SimpleMappedFile mappedFile = new SimpleMappedFile(file.getPath(), fileSize, startOffset);
            int wrote = mappedFile.recoverKeyIndexWrotePosition();
            if (wrote > 0 || queue.mappedFiles.isEmpty()) {
                queue.mappedFiles.add(mappedFile);
            } else {
                mappedFile.close();
            }
        }
        return queue;
    }

    /**
     * 获取最后一个映射文件，必要时创建新文件
     *
     * @param createIfAbsent true 且不存在时创建新文件
     */
    public synchronized SimpleMappedFile getLastMappedFile(boolean createIfAbsent) throws IOException {
        SimpleMappedFile mappedFile;
        if (mappedFiles.isEmpty()) {
            if (!createIfAbsent) {
                return null;
            }
            long startOffset = 0;
            String fileName = storePath + File.separator + formatFileName(startOffset);
            reclaimQuarantined(startOffset);
            mappedFile = new SimpleMappedFile(fileName, fileSize, startOffset);
            mappedFiles.add(mappedFile);
            return mappedFile;
        } else {
            mappedFile = mappedFiles.get(mappedFiles.size() - 1);
            if (mappedFile.isFull() && createIfAbsent) {
                long startOffset = mappedFile.getFileFromOffset() + fileSize;
                String fileName = storePath + File.separator + formatFileName(startOffset);
                reclaimQuarantined(startOffset);
                mappedFile = new SimpleMappedFile(fileName, fileSize, startOffset);
                mappedFiles.add(mappedFile);
            }
            return mappedFile;
        }
    }

    /**
     * 根据全局物理偏移找到对应的映射文件
     *
     * @param offset 全局物理偏移
     * @return 包含该偏移的文件，找不到时返回 null
     */
    public synchronized SimpleMappedFile findMappedFileByOffset(long offset) {
        if (mappedFiles.isEmpty()) {
            return null;
        }
        for (SimpleMappedFile mappedFile : mappedFiles) {
            long fileStart = mappedFile.getFileFromOffset();
            long fileEnd = fileStart + mappedFile.getWrotePosition();
            if (offset >= fileStart && offset < fileEnd) {
                return mappedFile;
            }
        }
        return null;
    }

    /**
     * 将物理偏移格式化成固定长度文件名
     * 方便按字典序排序即是按偏移排序
     */
    private String formatFileName(long offset) {
        return String.format("%020d", offset);
    }

    /**
     * 截断到指定全局字节位点:起始位点不早于 target 的尾部文件弹出并关闭,
     * 末文件的 wrote/flushed 下调到 target 之内。
     *
     * <p>被截断的数据必须<b>物理清零</b>:否则重启后的格式扫描会把旧 entry 重新
     * 采纳,出现"截断后又复活"。弹出文件整段清零,末文件从 target 起清零。</p>
     *
     * @param targetOffset 队列自身字节空间里的合法终点
     */
    public synchronized void truncateTo(long targetOffset) {
        while (!mappedFiles.isEmpty()) {
            SimpleMappedFile last = mappedFiles.get(mappedFiles.size() - 1);
            if (last.getFileFromOffset() >= targetOffset) {
                mappedFiles.remove(mappedFiles.size() - 1);
                last.zeroTailFrom(0);
                last.close();
            } else {
                int wrote = (int) (targetOffset - last.getFileFromOffset());
                last.setWrotePosition(wrote);
                last.setFlushedPosition(wrote);
                last.zeroTailFrom(wrote);
                break;
            }
        }
    }

    /**
     * 本次恢复发生全局前缀截断的终点物理位点
     *
     * @return 截断终点;-1 表示日志完整连续,未发生截断
     */
    public synchronized long getRecoveryTruncatedOffset() {
        return recoveryTruncatedOffset;
    }

    /**
     * 返回物理偏移所在文件的下一个文件起点;没有下一个文件时原样返回。
     *
     * <p>帧不跨文件,封存文件尾部可能存在"装不下下一帧"的浪费区。
     * IndexHost 游标落在该区域时用它跳到下一个文件起点,避免把零字节当数据/坏帧。</p>
     */
    public synchronized long nextFileStartOffset(long offset) {
        for (int i = 0; i < mappedFiles.size(); i++) {
            SimpleMappedFile file = mappedFiles.get(i);
            long start = file.getFileFromOffset();
            if (offset < start || offset >= start + fileSize) {
                continue;
            }
            return i + 1 < mappedFiles.size() ? mappedFiles.get(i + 1).getFileFromOffset() : offset;
        }
        return offset;
    }

    /**
     * 存储目录
     */
    public String getStorePath() {
        return storePath;
    }

    /**
     * 单个分段文件大小
     */
    public int getFileSize() {
        return fileSize;
    }

    /**
     * 获取当前队列中最大物理偏移量
     */
    public synchronized long getMaxOffset() {
        if (mappedFiles.isEmpty()) {
            return 0;
        }
        SimpleMappedFile last = mappedFiles.get(mappedFiles.size() - 1);
        return last.getFileFromOffset() + last.getWrotePosition();
    }

    /**
     * 刷新所有文件到磁盘
     *
     * @return 最后一个文件刷盘后的最大偏移
     */
    public synchronized long flush() {
        long flushed = 0;
        for (SimpleMappedFile mappedFile : mappedFiles) {
            flushed = mappedFile.getFileFromOffset() + mappedFile.flush();
        }
        return flushed;
    }

    /**
     * 关闭所有文件并清理列表
     */
    public synchronized void close() {
        for (SimpleMappedFile mappedFile : mappedFiles) {
            mappedFile.close();
        }
        mappedFiles.clear();
    }
}
