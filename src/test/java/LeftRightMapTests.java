import dev.mccue.left_right.LeftRightMap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class LeftRightMapTests {
    @Test
    public void writesOnlyPropagateOnRefresh() {
        final var map = LeftRightMap.<String, String>create();
        final var reader = map.readerFactory().createReader();
        final var writer = map.writer();

        assertNull(reader.get("a"));
        writer.put("a", "b");
        assertNull(reader.get("a"));
        writer.refresh();
        assertEquals(reader.get("a"), "b");
    }

    @Test
    public void tryWithResourcesWillRefresh() {
        final var map = LeftRightMap.<String, String>create();
        final var reader = map.readerFactory().createReader();

        try(final var writer = map.writer()) {
            writer.put("a", "b");
            assertNull(reader.get("a"));
        }
        assertEquals(reader.get("a"), "b");
    }

    @Test
    public void everyReaderSeesChangesAfterRefresh() {
        final var map = LeftRightMap.<String, String>create();
        final var readers = List.of(
                map.readerFactory().createReader(),
                map.readerFactory().createReader(),
                map.readerFactory().createReader(),
                map.readerFactory().createReader()
        );

        for (final var reader : readers) {
            assertNull(reader.get("a"));
        }

        try(final var writer = map.writer()) {
            writer.put("a", "b");
        }

        for (final var reader : readers) {
            assertEquals(reader.get("a"), "b");
        }
    }

    @Test
    public void readersOnDifferentThreadsSeeResults() {
        final var executor = Executors.newFixedThreadPool(8);
        final var map = LeftRightMap.<String, String>create();
        final var readers = List.of(
                map.readerFactory().createReader(),
                map.readerFactory().createReader(),
                map.readerFactory().createReader(),
                map.readerFactory().createReader(),
                map.readerFactory().createReader(),
                map.readerFactory().createReader(),
                map.readerFactory().createReader(),
                map.readerFactory().createReader()
        );

        try(final var writer = map.writer()) {
            writer.put("a", "b");
        }

        final List<Future<String>> readResults = new ArrayList<>();
        for (final var reader : readers) {
            readResults.add(executor.submit(() -> reader.get("a")));
        }

        assertEquals(
                List.of("b", "b", "b", "b","b", "b", "b", "b"),
                readResults.stream()
                    .map(res -> {
                        try {
                            return res.get();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toList())
        );

        executor.shutdownNow();
    }
}
