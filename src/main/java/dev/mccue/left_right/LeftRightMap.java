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
    private final ReaderFactory<K, V> readerFactory;
    private final ThreadSafeReader<K, V> threadSafeReader;
    private final Writer<K, V> writer;

    private LeftRightMap(ReaderFactory<K, V> readerFactory, Writer<K, V> writer) {
        this.readerFactory = readerFactory;
        this.threadSafeReader = new ThreadSafeReader<>(readerFactory);
        this.writer = writer;
    }

    /**
     * @return A thread safe factory for producing readers.
     */
    public ReaderFactory<K, V> readerFactory() {
        return this.readerFactory;
    }

    /**
     * @return A thread safe reader into the map. Uses thread locals to give each physical thread its own reader.
     */
    public ThreadSafeReader<K, V> threadSafeReader() {
        return this.threadSafeReader;
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
        final var readerFactory = new ReaderFactory<>(leftRight.readerFactory());
        final var writer = new Writer<>(leftRight.writer());
        return new LeftRightMap<>(readerFactory, writer);
    }

    public static final class ReaderFactory<K, V> {
        private final LeftRight.ReaderFactory<HashMap<K, V>> innerFactory;


        private ReaderFactory(LeftRight.ReaderFactory<HashMap<K, V>> innerFactory) {
            this.innerFactory = innerFactory;
        }

        public Reader<K, V> createReader() {
            return new Reader<>(this.innerFactory.createReader());
        }
    }

    public static final class Reader<K, V> {
        private final LeftRight.Reader<HashMap<K, V>> innerReader;

        private Reader(LeftRight.Reader<HashMap<K, V>> innerReader) {
            this.innerReader = innerReader;
        }

        public V get(K key) {
            return this.innerReader.read(map -> map.get(key));
        }

        public boolean containsKey(K key) {
            return this.innerReader.readBool(map -> map.containsKey(key));
        }

        public void forEach(BiConsumer<? super K, ? super V> action) {
            this.innerReader.readVoid(map -> map.forEach(action));
        }

        public int size() {
            return this.innerReader.readInt(HashMap::size);
        }

        public boolean isEmpty() {
            return this.innerReader.readBool(HashMap::isEmpty);
        }

        public boolean containsValue(V value) {
            return this.innerReader.readBool(map -> map.containsValue(value));
        }
    }

    public static final class ThreadSafeReader<K, V> {
        private final ThreadLocal<Reader<K, V>> localReader;

        private ThreadSafeReader(ReaderFactory<K, V> innerFactory) {
            this.localReader = ThreadLocal.withInitial(innerFactory::createReader);
        }


        public V get(K key) {
            return this.localReader.get().get(key);
        }

        public boolean containsKey(K key) {
            return this.localReader.get().containsKey(key);
        }

        public void forEach(BiConsumer<? super K, ? super V> action) {
            this.localReader.get().forEach(action);
        }

        public int size() {
            return this.localReader.get().size();
        }

        public boolean isEmpty() {
            return this.localReader.get().isEmpty();
        }

        public boolean containsValue(V value) {
            return this.localReader.get().containsValue(value);
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
            return this.innerWriter.write(new Put<>(key, value));
        }

        public V remove(K key) {
            return this.innerWriter.write(new Remove<>(key));
        }

        public void clear() {
            this.innerWriter.write(Clear.getInstance());
        }

        public int size() {
            return this.innerWriter.readInt(HashMap::size);
        }

        public boolean isEmpty() {
            return this.innerWriter.readBool(HashMap::isEmpty);
        }

        public boolean containsValue(V value) {
            return this.innerWriter.readBool(map -> map.containsValue(value));
        }

        public V get(K key) {
            return this.innerWriter.read(map -> map.get(key));
        }

        public boolean containsKey(K key) {
            return this.innerWriter.readBool(map -> map.containsKey(key));
        }

        public void forEach(BiConsumer<? super K, ? super V> action) {
            this.innerWriter.readVoid(map -> map.forEach(action));
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
