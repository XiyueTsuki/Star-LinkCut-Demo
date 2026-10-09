package com.xiyuetsuki.linkcutbackend.common;

/**
 * Base62 编解码工具
 * 字符集: [0-9a-zA-Z] 共62个字符
 * 用于将 Snowflake 生成的 Long ID 编码为短字符串
 */
public final class Base62Utils {

    private static final String BASE62_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private static final int BASE = 62;

    private Base62Utils() {
    }

    /**
     * 将 Long 型 ID 编码为 Base62 短字符串
     *
     * @param id Snowflake 雪花ID
     * @return Base62 编码后的短链码
     */
    public static String encode(long id) {
        if (id == 0) {
            return String.valueOf(BASE62_CHARS.charAt(0));
        }
        StringBuilder sb = new StringBuilder(10);
        long num = id;
        while (num > 0) {
            int remainder = (int) (num % BASE);
            sb.append(BASE62_CHARS.charAt(remainder));
            num /= BASE;
        }
        return sb.reverse().toString();
    }

    /**
     * 将 Base62 短字符串解码为 Long 型 ID
     *
     * @param shortCode Base62 短链码
     * @return 原始 Snowflake ID
     */
    public static long decode(String shortCode) {
        long result = 0;
        for (int i = 0; i < shortCode.length(); i++) {
            char c = shortCode.charAt(i);
            int value;
            if (c >= '0' && c <= '9') {
                value = c - '0';
            } else if (c >= 'A' && c <= 'Z') {
                value = c - 'A' + 10;
            } else if (c >= 'a' && c <= 'z') {
                value = c - 'a' + 36;
            } else {
                throw new IllegalArgumentException("Invalid Base62 character: " + c);
            }
            result = result * BASE + value;
        }
        return result;
    }
}