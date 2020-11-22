package com.company;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;



/**
 * A (hopefully) Fast, Thread safe map implementation inspired by Jon Gjengset's evmap
 * rust crate and talks on Youtube. Design is hypothetically optimized for lots of
 * concurrent reads.
 */
public final class EVMap {

    /**
     * We never actually construct an instance of EVMap. Readers and the Writer
     * just share object references constructed in the static method.
     */
    private EVMap() {}

    /**
     * A pair of a writer and a reader factory to the map.
     */
    public record ReaderWriterPair<K, V>(
            ReaderFactory<K, V> readerFactory,
            Writer<K, V> writer
    ) {}

    /**
     * Creates a writer and factory for readers.The reader factory can be asked
     * for any number of readers on any thread. The Writer is not thread safe and
     * must be owned by a single thread or otherwise coordinated.
     *
     * @param <K> The Key, assumed to be a valid HashMap key.
     * @param <V> The Value, assumed to be safe to share across threads.
     */
    public static <K, V> ReaderWriterPair<K, V> create() {
        final var readerEpochsMutex = new Object();
        final var readerEpochs = new ArrayList<AtomicLong>();
        final var readerMap = new HashMap<K, V>();
        final var readerMapRef = new AtomicReference<>(readerMap);
        final var writerMap = new HashMap<K, V>();

        final var readerFactory = new ReaderFactory<>(readerEpochsMutex, readerEpochs, readerMapRef);
        final var writer = new Writer<>(
                readerEpochsMutex,
                readerEpochs,
                readerMap,
                readerMapRef,
                writerMap
        );

        return new ReaderWriterPair<>(readerFactory, writer);
    }


    /**
     * Creates a reader to the underlying EVMap. This operation should be
     * totally threadsafe and efficient to do from any thread.
     */
    public static final class ReaderFactory<K, V> {
        private final Object readerEpochsMutex;
        private final List<AtomicLong> readerEpochs;
        private final AtomicReference<HashMap<K, V>> mapRef;

        private ReaderFactory(Object readerEpochsMutex,
                              List<AtomicLong> readerEpochs,
                              AtomicReference<HashMap<K, V>> mapRef) {
            this.readerEpochs = readerEpochs;
            this.readerEpochsMutex = readerEpochsMutex;
            this.mapRef = mapRef;
        }

        public Reader<K, V> createReader() {
            synchronized (this.readerEpochsMutex) {
                final var epoch = new AtomicLong();
                this.readerEpochs.add(epoch);
                return new Reader<K, V>(epoch, this.mapRef);
            }
        }
    }

    /**
     * A Reader to the Map
     * @param <K>
     * @param <V>
     */
    public static final class Reader<K, V> {
        private final AtomicReference<HashMap<K, V>> mapRef;
        private final AtomicLong epochCounter;

        private Reader(AtomicLong epochCounter, AtomicReference<HashMap<K, V>> mapRef) {
            this.epochCounter = epochCounter;
            this.mapRef = mapRef;
        }

        public V get(K key) {
            this.epochCounter.addAndGet(1);
            final var currentMap = mapRef.get();
            try {
                return currentMap.get(key);
            }
            finally {
                this.epochCounter.addAndGet(1);
            }
        }
    }

    /**
     * Represents an operation that can be performed on the inner map.
     * Useful for managing a log of operations performed.
     */
    sealed interface Operation<K, V> permits Put<K, V>, Remove<K, V> {
        void perform(HashMap<K, V> map);
    }

    /**
     * Insert a value into the map.
     */
    record Put<K, V>(K key, V value) implements Operation<K, V> {
        V put(HashMap<K, V> map) {
            return map.put(key, value);
        }

        @Override
        public void perform(HashMap<K, V> map) {
            this.put(map);
        }
    }

    /**
     * Remove some key from the map.
     */
    record Remove<K, V>(K key) implements Operation<K, V> {
        V remove(HashMap<K, V> map) {
            return map.remove(key);
        }

        @Override
        public void perform(HashMap<K, V> map) {
            this.remove(map);
        }
    }

    public static final class Writer<K, V> {
        /**
         * The log of operations performed on this map.
         */
        private final ArrayList<Operation<K, V>> opLog;

        /**
         * A mutex to use to synchronize all reads to the readerEpochs
         */
        private final Object readerEpochsMutex;

        /**
         * A list of all of the epoch counts for the readers.
         */
        private final List<AtomicLong> readerEpochs;

        /**
         * The map that readers should eventually be reading from.
         */
        private HashMap<K, V> readerMap;

        /**
         * The reference that readers should be looking at to pick the map to read from.
         */
        private final AtomicReference<HashMap<K, V>> readerMapRef;

        /**
         * The map that the writer is currently reading from.
         */
        private HashMap<K, V> writerMap;

        private Writer(Object readerEpochsMutex,
                       List<AtomicLong> readerEpochs,
                       HashMap<K, V> readerMap,
                       AtomicReference<HashMap<K, V>> readerMapRef,
                       HashMap<K, V> writerMap) {
            this.opLog = new ArrayList<>();
            this.readerEpochsMutex = readerEpochsMutex;
            this.readerEpochs = readerEpochs;
            this.readerMap = readerMap;
            this.readerMapRef = readerMapRef;
            this.writerMap = writerMap;
        }

        public V put(K key, V value) {
            final var op = new Put<>(key, value);
            this.opLog.add(op);
            return op.put(this.writerMap);
        }

        public V remove(K key) {
            final var op = new Remove<K, V>(key);
            this.opLog.add(op);
            return op.remove(this.writerMap);
        }

        /**
         * Propagates writes to readers.
         */
        public void refresh() {
            // Swap the pointer for the readers
            this.readerMapRef.set(this.writerMap);
            final var pivot = this.writerMap;
            this.writerMap = this.readerMap;
            this.readerMap = pivot;

            // Make sure readers have moved on
            synchronized (this.readerEpochsMutex) {
                List<AtomicLong> epochs = this.readerEpochs;
                while (epochs.size() != 0) {
                    List<AtomicLong> needToRetry = new ArrayList<>();
                    for (final var epoch : epochs) {
                        final var epochValue = epoch.get();
                        if (epochValue % 2 == 1) {
                            needToRetry.add(epoch);
                        }
                    }
                    epochs = needToRetry;
                }
            }

            // Apply operations to new map
            for (final var operation : this.opLog) {
                operation.perform(this.writerMap);
            }

            // Clear operation log
            this.opLog.clear();
        }
    }
}
