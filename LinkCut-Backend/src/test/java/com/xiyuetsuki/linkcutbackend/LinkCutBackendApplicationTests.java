package com.xiyuetsuki.linkcutbackend;

import com.xiyuetsuki.linkcutbackend.common.Base62Utils;
import com.xiyuetsuki.linkcutbackend.generator.ShortCodeGenerator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class LinkCutBackendApplicationTests {

    @Autowired
    private ShortCodeGenerator shortCodeGenerator;

    @Test
    void contextLoads() {
    }

    @Test
    void testSnowflakeShortCodeGenerate() {
        String code1 = shortCodeGenerator.generate();
        String code2 = shortCodeGenerator.generate();

        assertNotNull(code1);
        assertNotNull(code2);
        assertFalse(code1.isEmpty());
        assertFalse(code2.isEmpty());
        assertNotEquals(code1, code2);
        System.out.println("Generated short codes: " + code1 + ", " + code2);
    }

    @Test
    void testBase62EncodeDecode() {
        long id1 = 123456789L;
        long id2 = 9876543210L;
        long id3 = 1L;
        long id4 = 0L;

        String encoded1 = Base62Utils.encode(id1);
        String encoded2 = Base62Utils.encode(id2);
        String encoded3 = Base62Utils.encode(id3);
        String encoded4 = Base62Utils.encode(id4);

        assertEquals(id1, Base62Utils.decode(encoded1));
        assertEquals(id2, Base62Utils.decode(encoded2));
        assertEquals(id3, Base62Utils.decode(encoded3));
        assertEquals(id4, Base62Utils.decode(encoded4));

        System.out.println(id1 + " -> " + encoded1 + " -> " + Base62Utils.decode(encoded1));
        System.out.println(id2 + " -> " + encoded2 + " -> " + Base62Utils.decode(encoded2));
    }

    @Test
    void testShortCodeLength() {
        for (int i = 0; i < 10; i++) {
            String code = shortCodeGenerator.generate();
            assertTrue(code.length() >= 6 && code.length() <= 9,
                    "Short code length should be between 6 and 9, but got " + code.length());
        }
    }
}