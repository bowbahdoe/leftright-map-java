import dev.mccue.left_right.LeftRightMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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

        try (final var writer = map.writer()) {
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

        try (final var writer = map.writer()) {
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

        try (final var writer = map.writer()) {
            writer.put("a", "b");
        }

        final List<Future<String>> readResults = new ArrayList<>();
        for (final var reader : readers) {
            readResults.add(executor.submit(() -> reader.get("a")));
        }

        assertEquals(
                List.of("b", "b", "b", "b", "b", "b", "b", "b"),
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

    @Test
    public void writerSeesChangesImmediately() {
        final var map = LeftRightMap.<String, String>create();

        try (final var writer = map.writer()) {
            writer.put("a", "b");
            writer.put("b", "c");
            if (writer.get("a") != null) {
                writer.put("e", "f");
            }

            assertEquals(writer.get("a"), "b");
            assertEquals(writer.get("b"), "c");
            assertEquals(writer.get("e"), "f");
        }
    }

    @Test
    public void differentOperationsAreAppliedInOrder() {
        final var map = LeftRightMap.<String, String>create();
        final var reader = map.readerFactory().createReader();
        final var writer = map.writer();
        writer.put("a", "b");
        writer.clear();
        writer.put("c", "d");
        writer.remove("c");
        writer.put("e", "f");
        writer.refresh();

        assertEquals(reader.size(), 1);
        assertEquals(reader.get("e"), "f");
    }

    @Test
    public void noIntermediateResultsAreSeenByReaders() {
        final var map = LeftRightMap.<String, String>create();
        final var writer = map.writer();
        writer.put("a", "b");
        writer.refresh();


        final var executor = Executors.newFixedThreadPool(8);
        final List<Future<String>> readResults = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            final var reader = map.readerFactory().createReader();
            readResults.add(executor.submit(() -> reader.get("a")));
        }

        writer.put("a", "c");
        writer.put("a", "d");
        writer.refresh();


        assertEquals(
                Set.of("b", "d"), // spawning the futures should always take long enough to see the final state.
                readResults.stream()
                        .map(res -> {
                            try {
                                return res.get();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .collect(Collectors.toSet())
        );

        executor.shutdownNow();
    }
}
