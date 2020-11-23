package dev.mccue.left_right;

import java.io.Closeable;
import java.util.HashMap;
import java.util.function.BiConsumer;

/**
 * A (hopefully) Fast, (hopefully) Thread safe Map data structure inspired by Jon Gjengset's evmap
 * rust crate and talks on Youtube. Design is hypothetically optimized for lots of
 * concurrent reads.
 *
 * Neither Readers or Writers implement java.util.Map at this stage, so if that is
 * a showstopper for you then 🤷.
 *
 * @param <K> The Key, assumed to be a valid immutable HashMap key.
 * @param <V> The Value, assumed to be safe to share across threads.
 */
public final class LeftRightMap<K, V> {
    private final Reader<K, V> reader;
    private final Writer<K, V> writer;

    private LeftRightMap(Reader<K, V> reader, Writer<K, V> writer) {
        this.reader = reader;
        this.writer = writer;
    }

    /**
     * @return A thread safe factory for producing readers.
     */
    public Reader<K, V> reader() {
        return this.reader;
    }

    /**
     * @return The writer for the map. Should only be used by a single thread.
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
    public static <K, V> LeftRightMap<K, V> create() {
        final var leftRight = LeftRight.<HashMap<K, V>>create(HashMap::new);
        final var readerPool = new Reader<>(leftRight.readerFactory());
        final var writer = new Writer<>(leftRight.writer());
        return new LeftRightMap<>(readerPool, writer);
    }

    /**
     * A Thread Safe reader into the map. Safe to share between threads as desired.
     */
    public static final class Reader<K, V> {
        /**
         * The readers in LeftRight aren't thread safe by themselves. What is safe
         * is to use each reader in a dedicated thread. We can simplify that process
         * somewhat via a thread locals so each thread gets its own dedicated reader.
         *
         * TODO: This will fall apart in a situation where the # and identity of threads changes a lot.
         * (Since the matching LeftRight.Writer will have an ever growing list of epochs to traverse)
         */
        private final ThreadLocal<LeftRight.Reader<HashMap<K, V>>> innerReader;

        private Reader(LeftRight.ReaderFactory<HashMap<K, V>> innerFactory) {
            this.innerReader = ThreadLocal.withInitial(innerFactory::createReader);
        }

        public V get(K key) {
            return this.innerReader.get().performRead(map -> map.get(key));
        }

        public boolean containsKey(K key) {
            return this.innerReader.get().performRead(map -> map.containsKey(key));
        }

        public void forEach(BiConsumer<? super K, ? super V> action) {
            this.innerReader.get().performRead(map -> {
                map.forEach(action);
                return null;
            });
        }

        public int size() {
            return this.innerReader.get().performRead(HashMap::size);
        }

        public boolean isEmpty() {
            return this.innerReader.get().performRead(HashMap::isEmpty);
        }

        public boolean containsValue(V value) {
            return this.innerReader.get().performRead(map -> map.containsValue(value));
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


    /**
     * A writer into the Map.
     * <p>
     *     This is not thread safe, so either a single thread needs to have ownership of the writer
     *     or access to the writer needs to be coordinated via some other mechanism.
     * </p>
     *
     * <p>
     *     All writes done are only propagated to readers when {@link Writer#refresh()}
     *     or {@link Writer#close()} are called.
     * </p>
     *
     * <p>
     *     Any reads done via the writer will by definition always get the most up to date state of the map.
     * </p>
     */
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
