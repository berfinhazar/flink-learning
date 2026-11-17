package com.berfin.flink;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.*;

/** Deterministic bounded replay of the course bank dataset. */
public class BankFraudAnalysis {
    static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
        .withResolverStyle(java.time.format.ResolverStyle.STRICT);
    public static void main(String[] args) throws Exception {
        Path data = Path.of(args.length > 0 ? args[0] : "data/course");
        Path output = Path.of(args.length > 1 ? args[1] : "output/course-alerts.csv");
        ZoneId zone = ZoneId.of(args.length > 2 ? args[2] : "Asia/Kolkata");
        long delay = args.length > 3 ? Long.parseLong(args[3]) : 0;
        if(delay < 0) throw new IllegalArgumentException("Replay delay must be non-negative");
        Set<String> customers = new HashSet<>(), cards = new HashSet<>();
        for (String line : lines(data.resolve("alarmed_cust.txt"))) customers.add(fields(line, 2)[0]);
        for (String line : lines(data.resolve("lost_cards.txt"))) cards.add(fields(line, 4)[0]);
        List<Transaction> transactions = new ArrayList<>();
        long sequence = 0;
        for (String line : lines(data.resolve("bank_data.txt"))) {
            String[] f = fields(line, 7);
            Transaction t = new Transaction();
            t.sequence = ++sequence; t.id=f[0]; t.customer=f[3]; t.account=f[4]; t.card=f[5]; t.city=f[2];
            t.time=LocalDateTime.parse(f[1], TIME).atZone(zone).toInstant().toEpochMilli();
            t.amount=new BigDecimal(f[6]).toPlainString();
            transactions.add(t);
        }
        if (transactions.isEmpty()) throw new IllegalArgumentException("No transactions");
        transactions.sort(Comparator.comparingLong((Transaction t) -> t.time).thenComparingLong(t -> t.sequence));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        var alerts = env.fromData(transactions.toArray(Transaction[]::new))
            .name("Course transactions - chronological replay").uid("bank-input")
            .map(new ReplayPace(delay)).name("Replay pacing")
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Transaction>forMonotonousTimestamps()
                .withTimestampAssigner((t, previous) -> t.time))
            .keyBy(t -> t.customer)
            .process(new FraudDetector(customers, cards))
            .name("Four fraud rules - rolling 60 seconds").uid("fraud-rules");
        Files.createDirectories(output.toAbsolutePath().getParent());
        int count=0;
        try (CloseableIterator<String> it = alerts.executeAndCollect("Bank Fraud Analysis");
             var writer = Files.newBufferedWriter(output)) {
            writer.write("input_row,transaction_id,time_utc,customer_id,card_masked,amount,city,reason,transactions_60s,cities_60s\n");
            while(it.hasNext()) { String row=it.next(); writer.write(row+"\n"); System.out.println(row); count++; }
        }
        System.out.printf("Processed %d input rows; emitted %d rule alerts. Output: %s%n", transactions.size(), count, output.toAbsolutePath());
    }
    static List<String> lines(Path path) throws Exception {
        return Files.readAllLines(path).stream().filter(s -> !s.isBlank()).toList();
    }
    static String[] fields(String line, int expected) {
        String[] f=line.split(",", -1);
        if(f.length!=expected) throw new IllegalArgumentException("Expected "+expected+" columns");
        for(int i=0;i<f.length;i++) {f[i]=f[i].trim(); if(f[i].isEmpty()) throw new IllegalArgumentException("Empty column "+i);}
        return f;
    }
    public static class ReplayPace implements org.apache.flink.api.common.functions.MapFunction<Transaction,Transaction> {
        private final long delay;
        public ReplayPace(long delay) { this.delay=delay; }
        @Override public Transaction map(Transaction t) throws Exception {
            if(delay>0) Thread.sleep(delay);
            return t;
        }
    }
    public static class Transaction implements Serializable {
        public long sequence, time;
        public String id, customer, account, card, city, amount;
        public Transaction() {}
    }
    public static class Recent implements Serializable {
        public long time;
        public String city;
        public Recent() {}
        Recent(long time,String city) {this.time=time;this.city=city;}
    }
    public static class FraudDetector extends KeyedProcessFunction<String,Transaction,String> {
        private final Set<String> customers, cards;
        private transient ListState<Recent> recent;
        public FraudDetector(Set<String> customers,Set<String> cards) {this.customers=customers;this.cards=cards;}
        @Override public void open(OpenContext context) throws Exception {
            recent=getRuntimeContext().getListState(new ListStateDescriptor<>("recent-transactions",Types.POJO(Recent.class)));
        }
        @Override public void processElement(Transaction t, Context ctx, Collector<String> out) throws Exception {
            List<Recent> active = new ArrayList<>();
            for(Recent r:recent.get()) if(r.time>t.time-60000) active.add(r);
            active.add(new Recent(t.time,t.city)); recent.update(active);
            ctx.timerService().registerEventTimeTimer(t.time+60000);
            long cities=active.stream().map(r -> r.city.toLowerCase(Locale.ROOT)).distinct().count();
            List<String> reasons=new ArrayList<>();
            if(customers.contains(t.customer)) reasons.add("ALARMED_CUSTOMER");
            if(cards.contains(t.card)) reasons.add("LOST_CARD");
            if(active.size()>10) reasons.add("MORE_THAN_10_TRANSACTIONS_60S");
            if(cities>2) reasons.add("MORE_THAN_2_CITIES_60S");
            for(String reason:reasons) {
                String masked="****"+t.card.substring(Math.max(0,t.card.length()-4));
                out.collect(t.sequence+","+csv(t.id)+","+Instant.ofEpochMilli(t.time)+","+csv(t.customer)+","+csv(masked)
                    +","+t.amount+","+csv(t.city)+","+reason+","+active.size()+","+cities);
            }
        }
        @Override public void onTimer(long timestamp,OnTimerContext ctx,Collector<String> out) throws Exception {
            List<Recent> active=new ArrayList<>();
            for(Recent r:recent.get()) if(r.time>timestamp-60000) active.add(r);
            if(active.isEmpty()) recent.clear(); else recent.update(active);
        }
        static String csv(String value) { return "\""+value.replace("\"","\"\"")+"\""; }
    }
}
