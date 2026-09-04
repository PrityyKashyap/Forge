package com.forge.storage.bloom;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BloomFilterFileTest {

    @Test
    void roundTripsAFilter(@TempDir Path dir) throws IOException {
        BloomFilter original = BloomFilter.sizedFor(100, 0.01);
        for (int i = 0; i < 100; i++) {
            original.add("k" + i);
        }

        Path sstable = dir.resolve("sstable-1.sst");
        Files.writeString(sstable, "pretend sstable bytes");
        Path temp = dir.resolve("bloom.tmp");
        BloomFilterFile.write(temp, BloomFilterFile.sidecarPathFor(sstable), original);

        assertTrue(Files.notExists(temp), "temp file must not remain after a successful atomic rename");
        assertTrue(Files.exists(BloomFilterFile.sidecarPathFor(sstable)));

        Optional<BloomFilter> loaded = BloomFilterFile.tryLoad(sstable);
        assertTrue(loaded.isPresent());
        for (int i = 0; i < 100; i++) {
            assertTrue(loaded.get().mightContain("k" + i));
        }
    }

    @Test
    void missingSidecarLoadsAsEmpty(@TempDir Path dir) {
        Path sstable = dir.resolve("sstable-no-sidecar.sst");
        assertTrue(BloomFilterFile.tryLoad(sstable).isEmpty());
    }

    @Test
    void corruptSidecarLoadsAsEmptyRatherThanThrowing(@TempDir Path dir) throws IOException {
        Path sstable = dir.resolve("sstable-2.sst");
        Path sidecar = BloomFilterFile.sidecarPathFor(sstable);
        Files.write(sidecar, new byte[]{1, 2, 3}); // far too short to be a real header

        assertFalse(BloomFilterFile.tryLoad(sstable).isPresent());
    }

    @Test
    void bitFlipInBodyIsDetectedAndTreatedAsAbsent(@TempDir Path dir) throws IOException {
        BloomFilter original = BloomFilter.sizedFor(50, 0.01);
        for (int i = 0; i < 50; i++) {
            original.add("v" + i);
        }
        Path sstable = dir.resolve("sstable-3.sst");
        Path sidecar = BloomFilterFile.sidecarPathFor(sstable);
        BloomFilterFile.write(dir.resolve("bloom.tmp"), sidecar, original);

        try (RandomAccessFile raf = new RandomAccessFile(sidecar.toFile(), "rw")) {
            long lastByte = raf.length() - 1;
            raf.seek(lastByte);
            int b = raf.read();
            raf.seek(lastByte);
            raf.write(b ^ 0xFF);
        }

        assertFalse(BloomFilterFile.tryLoad(sstable).isPresent());
    }

    @Test
    void sidecarPathIsSstablePathPlusBloomSuffix() {
        Path sstable = Path.of("/data/sstable-42.sst");
        assertEquals(Path.of("/data/sstable-42.sst.bloom"), BloomFilterFile.sidecarPathFor(sstable));
    }
}
