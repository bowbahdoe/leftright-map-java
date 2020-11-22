package dev.mccue.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
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
    public ReaderFactory<DS> readerFactory() {
        return this.readerFactory;
    }

    /**
     * @return The writer for the map.
     */
    public Writer<DS> writer() {
        return this.writer;
    }

    /**
     * Creates a writer and factory for readers.The reader factory can be asked
     * for any number of readers on any thread. The Writer is not thread safe and
     * must be owned by a single thread or otherwise coordinated.
     */
    public static <DS> LeftRight<DS> create(Supplier<DS> createDS) {
        final var readerEpochs = new ArrayList<AtomicLong>();
        final var readerDS = createDS.get();
        final var readerDSRef = new AtomicReference<>(readerDS);
        final var writerDS = createDS.get();

        final var readerFactory = new ReaderFactory<>(
                readerEpochs,
                readerDSRef
        );

        final var writer = new Writer<>(
                readerEpochs,
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
    public static final class ReaderFactory<DS> {
        private final List<AtomicLong> readerEpochs;
        private final AtomicReference<DS> dsRef;

        private ReaderFactory(List<AtomicLong> readerEpochs,
                              AtomicReference<DS> dsRef) {
            this.readerEpochs = readerEpochs;
            this.dsRef = dsRef;
        }

        /**
         * @return A new reader. This Reader is **not** thread-safe. For each thread that wants to read,
         * they should create their own readers with this factory or synchronize usage some other way.
         */
        public Reader<DS> createReader() {
            synchronized (this.readerEpochs) {
                final var epoch = new AtomicLong();
                this.readerEpochs.add(epoch);
                return new Reader<>(epoch, this.dsRef);
            }
        }
    }

    /**
     * A Reader to the Data Structure. Each reader must have only a single owner and is not
     * thread safe.
     */
    public static final class Reader<DS> {
        private final AtomicReference<DS> dsRef;
        private final AtomicLong epochCounter;

        private Reader(AtomicLong epochCounter, AtomicReference<DS> dsRef) {
            this.epochCounter = epochCounter;
            this.dsRef = dsRef;
        }

        public <T> T performRead(Function<DS, T> readOperation) {
            this.epochCounter.addAndGet(1);
            final var currentDS = dsRef.get();
            try {
                return readOperation.apply(currentDS);
            }
            finally {
                this.epochCounter.addAndGet(1);
            }
        }
    }



    /**
     * Represents a loggable and repeatable operation on a data structure.
     * @param <DS> The Data Structure the Operation works on.
     */
    public interface Operation<DS> {
        void perform(DS ds);
    }

    public static final class Writer<DS> {
        /**
         * The log of operations performed on this data structure in the current refresh cycle.
         */
        private final ArrayList<Operation<DS>> opLog;

        /**
         * A list of all of the epoch counts for the readers.
         */
        private final List<AtomicLong> readerEpochs;

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

        private Writer(List<AtomicLong> readerEpochs,
                       DS readerDS,
                       AtomicReference<DS> readerDSRef,
                       DS writerDS) {
            this.opLog = new ArrayList<>();
            this.readerEpochs = readerEpochs;
            this.readerDS = readerDS;
            this.readerDSRef = readerDSRef;
            this.writerDS = writerDS;
        }

        public void performWrite(Operation<DS> operation) {
            operation.perform(this.writerDS);
            this.opLog.add(operation);
        }

        public <T> T performRead(Function<DS, T> read) {
            return read.apply(this.writerDS);
        }

        /**
         * Propagates writes to readers.
         */
        public void refresh() {
            // Swap the pointer for the readers
            this.readerDSRef.set(this.writerDS);
            final var pivot = this.writerDS;
            this.writerDS = this.readerDS;
            this.readerDS = pivot;

            // Make sure readers have moved on
            synchronized (this.readerEpochs) {
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

            // Apply operations to new data structure
            for (final var operation : this.opLog) {
                operation.perform(this.readerDS);
            }

            // Clear operation log
            this.opLog.clear();
        }
    }
}
