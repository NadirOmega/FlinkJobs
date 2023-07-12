import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
/**
* Nad 
*/
public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // Configure incremental checkpointing
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointInterval(1000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // Configure state backend
        AbstractStateBackend stateBackend = new RocksDBStateBackend("file:///path/to/rocksdb");
        env.setStateBackend(stateBackend);

        // Generate a stream of credit card transactions
        DataStream<Transaction> transactions = env.addSource(new TransactionGenerator());

        // Generate a stream of fraud patterns
        DataStream<FraudPattern> fraudPatterns = env.fromElements(
                new FraudPattern("123456789", 2000),
                new FraudPattern("987654321", 1500)
        );

        // Broadcast the fraud patterns stream
        BroadcastStream<FraudPattern> broadcastFraudPatterns = fraudPatterns.broadcast();

        // Keyed stream by card number
        DataStream<String> alerts = transactions
                .keyBy(transaction -> transaction.getCardNumber())
                .connect(broadcastFraudPatterns)
                .process(new FraudDetectionProcessFunction())
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String alert) throws Exception {
                        // Perform an expensive stateful operation
                        StringBuilder transformedAlert = new StringBuilder();
                        for (char c : alert.toCharArray()) {
                            transformedAlert.append(Character.toUpperCase(c));
                        }
                        return transformedAlert.toString();
                    }
                })
                .keyBy(alert -> alert)
                .reduce(new ReduceFunction<String>() {
                    @Override
                    public String reduce(String alert1, String alert2) throws Exception {
                        // Perform another expensive stateful operation
                        return alert1 + ", " + alert2;
                    }
                });

        // Sink the fraud alerts to a file
        alerts.addSink(new FraudAlertFileSink("/path/to/fraud_alerts.txt"));

        // Execute the job
        env.execute("Fraud Detection Job");
    }

    // Custom source function to generate random credit card transactions
    private static class TransactionGenerator extends RichSourceFunction<Transaction> {
        private volatile boolean running = true;
        private Random random;

        @Override
        public void open(Configuration parameters) throws Exception {
            random = new Random();
        }

        @Override
        public void run(SourceContext<Transaction> ctx) throws Exception {
            while (running) {
                String cardNumber = generateRandomCardNumber();
                double amount = random.nextDouble() * 1000;
                long timestamp = System.currentTimeMillis();
                ctx.collect(new Transaction(cardNumber, amount, timestamp));
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        private String generateRandomCardNumber() {
            // Generate a random card number (for simplicity, we use a fixed prefix "1234" followed by random digits)
            StringBuilder cardNumber = new StringBuilder("1234");
            for (int i = 0; i < 12; i++) {
                cardNumber.append(random.nextInt(10));
            }
            return cardNumber.toString();
        }
    }

    // Custom ProcessFunction for fraud detection
    private static class FraudDetectionProcessFunction extends BroadcastProcessFunction<Transaction, FraudPattern, String> {
        private Map<String, Double> fraudThresholds = new HashMap<>();

        @Override
        public void processElement(Transaction transaction, ReadOnlyContext ctx, Collector<String> out) throws Exception {
            // Check if the card number is in the fraud patterns
            if (fraudThresholds.containsKey(transaction.getCardNumber())) {
                double fraudThreshold = fraudThresholds.get(transaction.getCardNumber());

                // Compare the transaction amount with the fraud threshold
                if (transaction.getAmount() > fraudThreshold) {
                    String fraudAlert = "Potential fraud detected! Card: " + transaction.getCardNumber()
                            + ", Amount: " + transaction.getAmount()
                            + ", Timestamp: " + transaction.getTimestamp();
                    out.collect(fraudAlert);
                }
            }
        }

        @Override
        public void processBroadcastElement(FraudPattern pattern, Context ctx, Collector<String> out) throws Exception {
            // Update the fraud threshold for the card number
            fraudThresholds.put(pattern.getCardNumber(), pattern.getFraudThreshold());
        }
    }

    // Transaction class
    public static class Transaction {
        private String cardNumber;
        private double amount;
        private long timestamp;

        public Transaction(String cardNumber, double amount, long timestamp) {
            this.cardNumber = cardNumber;
            this.amount = amount;
            this.timestamp = timestamp;
        }

        public String getCardNumber() {
            return cardNumber;
        }

        public double getAmount() {
            return amount;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    // FraudPattern class
    public static class FraudPattern {
        private String cardNumber;
        private double fraudThreshold;

        public FraudPattern(String cardNumber, double fraudThreshold) {
            this.cardNumber = cardNumber;
            this.fraudThreshold = fraudThreshold;
        }

        public String getCardNumber() {
            return cardNumber;
        }

        public double getFraudThreshold() {
            return fraudThreshold;
        }
    }

    // Sink function to write fraud alerts to a file
    private static class FraudAlertFileSink implements SinkFunction<String> {
        private final String filePath;

        public FraudAlertFileSink(String filePath) {
            this.filePath = filePath;
        }

        @Override
        public void invoke(String value, Context context) throws IOException {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
                writer.write(value);
                writer.newLine();
            }
        }
    }
}
