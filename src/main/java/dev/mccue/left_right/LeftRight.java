package dev.mccue.left_right;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

/**
 * A Left-Right Concurrency primitive.
 *
 * https://www.youtube.com/watch?v=eLNAMEoKAAc
 */
final class LeftRight<DS> {
    private final ReaderFactory<DS> readerFactory;
    private final Writer<DS> writer;

    private LeftRight(ReaderFactory<DS> readerFactory, Writer<DS> writer) {
        this.readerFactory = readerFactory;
        this.writer = writer;
    }

    /**
     * @return A thread safe factory for producing readers.
     */
    ReaderFactory<DS> readerFactory() {
        return this.readerFactory;
    }

    /**
     * @return The writer for the map.
     */
    Writer<DS> writer() {
        return this.writer;
    }

    /**
     * Creates a writer and factory for readers.The reader factory can be asked
     * for any number of readers on any thread. The Writer is not thread safe and
     * must be owned by a single thread or otherwise coordinated.
     */
    static <DS> LeftRight<DS> create(Supplier<DS> createDS) {
        final var readers = new ArrayList<Reader<DS>>();
        final var readerDS = createDS.get();
        final var readerDSRef = new AtomicReference<>(readerDS);
        final var writerDS = createDS.get();

        final var readerFactory = new ReaderFactory<>(
                readers,
                readerDSRef
        );

        final var writer = new Writer<>(
                readers,
                readerDS,
                readerDSRef,
                writerDS
        );

        return new LeftRight<>(readerFactory, writer);
    }


    /**
     * Creates a reader to the underlying Data structure. This operation should be
     * totally threadsafe and efficient to do from any thread.
     */
    static final class ReaderFactory<DS> {
        private final ArrayList<Reader<DS>> readers;
        private final AtomicReference<DS> dsRef;

        private ReaderFactory(ArrayList<Reader<DS>> readers,
                              AtomicReference<DS> dsRef) {
            this.readers = readers;
            this.dsRef = dsRef;
        }

        /**
         * @return A new reader. This Reader is **not** thread-safe. For each thread that wants to read,
         * they should create their own readers with this factory or synchronize usage some other way.
         */
        Reader<DS> createReader() {
            final var reader = new Reader<>(this.dsRef);
            synchronized (this.readers) {
                this.readers.add(reader);
            }
            return reader;
        }
    }

    /**
     * A Reader to the Data Structure. Each reader must have only a single owner and is not
     * thread safe.
     */
    static final class Reader<DS> {
        final AtomicReference<DS> dsRef;
        private final AtomicLong epoch;

        private Reader(AtomicReference<DS> dsRef) {
            this.epoch = new AtomicLong(0);
            this.dsRef = dsRef;
        }

        private long epoch() {
            return this.epoch.get();
        }

        void incrementEpoch() {
            this.epoch.incrementAndGet();
        }

        <T> T read(Function<DS, T> readOperation) {
            this.incrementEpoch();
            final var currentDS = dsRef.get();
            try {
                return readOperation.apply(currentDS);
            }
            finally {
                this.incrementEpoch();
            }
        }

        int readInt(ToIntFunction<DS> readOperation) {
            this.incrementEpoch();
            final var currentDS = dsRef.get();
            try {
                return readOperation.applyAsInt(currentDS);
            }
            finally {
                this.incrementEpoch();
            }
        }

        boolean readBool(Predicate<DS> readOperation) {
            this.incrementEpoch();
            final var currentDS = dsRef.get();
            try {
                return readOperation.test(currentDS);
            }
            finally {
                this.incrementEpoch();
            }
        }

        void readVoid(Consumer<DS> readOperation) {
            this.incrementEpoch();
            final var currentDS = dsRef.get();
            try {
                readOperation.accept(currentDS);
            }
            finally {
                this.incrementEpoch();
            }
        }
    }



    /**
     * Represents a loggable and repeatable operation on a data structure.
     * @param <DS> The Data Structure the Operation works on.
     */
    interface Operation<DS, R> {
        R perform(DS ds);
    }

    static final class Writer<DS> {
        /**
         * The log of operations performed on this data structure in the current refresh cycle.
         */
        private final ArrayList<Operation<DS, ?>> opLog;

        /**
         * A list of all of the epoch counts for the readers.
         */
        private final ArrayList<Reader<DS>> readers;

        /**
         * The data structure that readers should eventually be reading from.
         */
        private DS readerDS;

        /**
         * The reference that readers should be looking at to pick the data structure to read from.
         */
        private final AtomicReference<DS> readerDSRef;

        /**
         * The data structure that the writer is currently reading from.
         */
        private DS writerDS;

        private Writer(ArrayList<Reader<DS>> readers,
                       DS readerDS,
                       AtomicReference<DS> readerDSRef,
                       DS writerDS) {
            this.opLog = new ArrayList<>();
            this.readers = readers;
            this.readerDS = readerDS;
            this.readerDSRef = readerDSRef;
            this.writerDS = writerDS;
        }

        <R> R write(Operation<DS, R> operation) {
            R res = operation.perform(this.writerDS);
            this.opLog.add(operation);
            return res;
        }

        <T> T read(Function<DS, T> readOperation) {
            return readOperation.apply(this.writerDS);
        }

        int readInt(ToIntFunction<DS> readOperation) {
            return readOperation.applyAsInt(this.writerDS);
        }

        boolean readBool(Predicate<DS> readOperation) {
            return readOperation.test(this.writerDS);
        }

        void readVoid(Consumer<DS> readOperation) {
            readOperation.accept(this.writerDS);
        }

        /**
         * Propagates writes to readers.
         */
        void refresh() {
            // Swap the pointer for the readers
            this.readerDSRef.set(this.writerDS);
            final var pivot = this.writerDS;
            this.writerDS = this.readerDS;
            this.readerDS = pivot;

            // Track the last epoch we read from the readers.
            final class StillReadingReader {
                private final Reader<?> reader;
                private final long previousEpoch;

                StillReadingReader(Reader<?> reader, long previousEpoch) {
                    this.previousEpoch = previousEpoch;
                    this.reader = reader;
                }

                boolean hasMovedOn() {
                    return this.reader.epoch() != this.previousEpoch;
                }
            }

            // Make sure readers have moved on
            synchronized (this.readers) { // No new readers while we are refreshing.
                var readers = this.readers;
                var stillReading = new ArrayList<StillReadingReader>();
                for (final var reader : readers) {
                    final var epochValue = reader.epoch();
                    if (epochValue % 2 == 1) {
                        stillReading.add(new StillReadingReader(reader, epochValue));
                    }
                }

                while (stillReading.size() != 0) {
                    final var needToRetry = new ArrayList<StillReadingReader>();
                    for (final var reader : stillReading) {
                        if (!reader.hasMovedOn()) {
                            needToRetry.add(reader);
                        }
                    }
                    stillReading = needToRetry;
                }
            }

            // Apply operations to new data structure
            for (final var operation : this.opLog) {
                operation.perform(this.readerDS);
            }

            // Clear operation log
            this.opLog.clear();
        }
    }
}
