package com.ispengya.file.index;

/**
 * 业务 key 的 32 位无符号 hash 约定,与帧头/KeyIndex 的存储方式保持一致
 */
public final class KeyHash {

    private KeyHash() {
    }

    /**
     * 计算 key 的 32 位无符号 hash(0 保留给"无 key")
     */
    public static long of(String key) {
        return key.hashCode() & 0xffffffffL;
    }
}
