package com.ispengya.file.api;

/**
 * 存储目录已被另一进程(或本进程另一实例)独占时抛出
 *
 * <p>mini-file-storage 采用单写者模型:同一 baseDir 同时只允许一个 {@link MiniFileStore}
 * 实例打开,通过目录下的 .store.lock 文件锁实现。双开会绕过单写者假设,
 * 两个实例各自恢复、从同一位置追加并互相覆盖,必须在启动时直接拒绝。</p>
 */
public class StoreLockHeldException extends RuntimeException {

    /**
     * 锁文件路径
     */
    private final String lockFilePath;

    public StoreLockHeldException(String lockFilePath) {
        super("store directory is already held by another instance, lock file: " + lockFilePath);
        this.lockFilePath = lockFilePath;
    }

    public String getLockFilePath() {
        return lockFilePath;
    }
}
