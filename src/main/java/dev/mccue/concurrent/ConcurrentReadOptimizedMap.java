package dev.mccue.concurrent;

import java.io.Closeable;
import java.util.HashMap;
import java.util.function.BiConsumer;

/**
 * A (hopefully) Fast, Thread safe map implementation inspired by Jon Gjengset's evmap
 * rust crate and talks on Youtube. Design is hypothetically optimized for lots of
 * concurrent reads.
 *
 * Unlike evmap, this is not a multi value map. I'm uncertain if it should be.
 *
 * @param <K> The Key, assumed to be a valid immutable HashMap key.
 * @param <V> The Value, assumed to be safe to share across threads.
 */
public final class ConcurrentReadOptimizedMap<K, V> {
    private final ReaderFactory<K, V> readerFactory;
    private final Writer<K, V> writer;

    private ConcurrentReadOptimizedMap(ReaderFactory<K, V> readerFactory, Writer<K, V> writer) {
        this.readerFactory = readerFactory;
        this.writer = writer;
    }

    /**
     * @return A thread safe factory for producing readers.
     */
    public ReaderFactory<K, V> readerFactory() {
        return this.readerFactory;
    }

    /**
     * @return The writer for the map.
     */
    public Writer<K, V> writer() {
        return this.writer;
    }

    /**
     * Creates a writer and factory for readers.The reader factory can be asked
     * for any number of readers on any thread. The Writer is not thread safe and
     * must be owned by a single thread or otherwise coordinated.
     *
     * @param <K> The Key, assumed to be a valid immutable HashMap key.
     * @param <V> The Value, assumed to be safe to share across threads.
     */
    public static <K, V> ConcurrentReadOptimizedMap<K, V> create() {
        LeftRight<HashMap<K, V>> leftRight = LeftRight.create(HashMap::new);

        final var readerFactory = new ReaderFactory<>(leftRight.readerFactory());

        final var writer = new Writer<>(leftRight.writer());

        return new ConcurrentReadOptimizedMap<>(readerFactory, writer);
    }


    /**
     * Creates a reader to the underlying EVMap. This operation should be
     * totally threadsafe and efficient to do from any thread.
     */
    public static final class ReaderFactory<K, V> {
        private final LeftRight.ReaderFactory<HashMap<K, V>> innerFactory;

        private ReaderFactory(LeftRight.ReaderFactory<HashMap<K, V>> innerFactory) {
            this.innerFactory = innerFactory;
        }

        /**
         * @return A new reader. This Reader is **not** thread-safe. For each thread that wants to read,
         * they should create their own readers with this factory or synchronize usage some other way.
         */
        public Reader<K, V> createReader() {
            return new Reader<>(this.innerFactory.createReader());
        }
    }

    /**
     * A Reader to the Map. Each reader must have only a single owner and is not
     * thread safe.
     */
    public static final class Reader<K, V> {
        private final LeftRight.Reader<HashMap<K, V>> innerReader;

        private Reader(LeftRight.Reader<HashMap<K, V>> innerReader) {
            this.innerReader = innerReader;
        }

        public V get(K key) {
            return this.innerReader.performRead(map -> map.get(key));
        }

        public boolean containsKey(K key) {
            return this.innerReader.performRead(map -> map.containsKey(key));
        }

        public void forEach(BiConsumer<? super K, ? super V> action) {
            this.innerReader.performRead(map -> {
                map.forEach(action);
                return null;
            });
        }

        public int size() {
            return this.innerReader.performRead(HashMap::size);
        }

        public boolean isEmpty() {
            return this.innerReader.performRead(HashMap::isEmpty);
        }

        public boolean containsValue(V value) {
            return this.innerReader.performRead(map -> map.containsValue(value));
        }
    }

    /**
     * Insert a value into the map.
     */
    static final class Put<K, V> implements LeftRight.Operation<HashMap<K, V>, V>  {
        private final K key;
        private final V value;

        public Put(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public V perform(HashMap<K, V> map) {
            return map.put(key, value);
        }
    }

    /**
     * Remove some key from the map.
     */
    static final class Remove<K, V> implements LeftRight.Operation<HashMap<K, V>, V>  {
        private final K key;

        public Remove(K key) {
            this.key = key;
        }

        @Override
        public V perform(HashMap<K, V> map) {
            return map.remove(key);
        }
    }

    /**
     * Clears all entries from the map.
     */
    static final class Clear<K, V> implements LeftRight.Operation<HashMap<K, V>, Void>  {
        private static final Clear<?, ?> INSTANCE = new Clear<>();

        private Clear() {}

        @SuppressWarnings("unchecked")
        public static <K, V> Clear<K, V> getInstance() {
            return (Clear<K, V>) INSTANCE;
        }

        @Override
        public Void perform(HashMap<K, V> map) {
            map.clear();
            return null;
        }
    }


    public static final class Writer<K, V> implements Closeable {
        private final LeftRight.Writer<HashMap<K, V>> innerWriter;

        private Writer(LeftRight.Writer<HashMap<K, V>> innerWriter) {
            this.innerWriter = innerWriter;
        }

        public V put(K key, V value) {
            return this.innerWriter.performWrite(new Put<>(key, value));
        }

        public V remove(K key) {
            return this.innerWriter.performWrite(new Remove<>(key));
        }

        public void clear() {
            this.innerWriter.performWrite(Clear.getInstance());
        }

        public int size() {
            return this.innerWriter.performRead(HashMap::size);
        }

        public boolean isEmpty() {
            return this.innerWriter.performRead(HashMap::isEmpty);
        }

        public boolean containsValue(V value) {
            return this.innerWriter.performRead(map -> map.containsValue(value));
        }

        public V get(K key) {
            return this.innerWriter.performRead(map -> map.get(key));
        }

        public boolean containsKey(K key) {
            return this.innerWriter.performRead(map -> map.containsKey(key));
        }

        public void forEach(BiConsumer<? super K, ? super V> action) {
            this.innerWriter.performRead(map -> {
                map.forEach(action);
                return null;
            });
        }

        /**
         * Propagates writes to readers.
         */
        public void refresh() {
            this.innerWriter.refresh();
        }

        /**
         * A close() implementation that calls refresh() for convenient use with try-with-resources.
         *
         * <pre>
         * {@code
         * final var map = ConcurrentReadOptimizedMap.<Integer, Integer>create();
         * try (final var writer = map.writer()) { // Writes will be propagated at the end of scope.
         *     int key = 0;
         *     if (writer.containsKey(1)) {
         *         writer.put(writer.get(1) + 1);
         *     }
         *     else {
         *         writer.put(1, 0);
         *     }
         * }
         * </pre>
         */
        @Override
        public void close() {
            this.refresh();
        }
    }
}
