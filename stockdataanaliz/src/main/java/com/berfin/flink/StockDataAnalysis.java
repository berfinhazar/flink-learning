package com.berfin.flink;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.*;
import java.time.*;
import java.time.format.*;
import java.util.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.*;

public class StockDataAnalysis {
    private static final DateTimeFormatter INPUT_TIME = DateTimeFormatter.ofPattern("dd/MM/uuuu HH:mm:ss")
        .withResolverStyle(ResolverStyle.STRICT);

    public static void main(String[] args) throws Exception {
        Path input=Path.of(args.length>0?args[0]:"data/FUTURES_TRADES.txt");
        Path output=Path.of(args.length>1?args[1]:"output");
        ZoneId zone=ZoneId.of(args.length>2?args[2]:"UTC");
        String symbol=args.length>3?args[3]:"XYZ";
        BigDecimal threshold=new BigDecimal(args.length>4?args[4]:"5");
        long delay=args.length>5?Long.parseLong(args[5]):0;
        if(!symbol.matches("[A-Za-z0-9._-]+")) throw new IllegalArgumentException("Invalid symbol");
        if(threshold.signum()<0 || delay<0) throw new IllegalArgumentException("Threshold and delay must be non-negative");
        List<Tick> ticks=new ArrayList<>();
        long previous=Long.MIN_VALUE;
        int row=0;
        for(String line:Files.readAllLines(input)) {
            row++;
            if(line.isBlank()) continue;
            try {
                String[] f=line.split(",",-1);
                if(f.length!=4) throw new IllegalArgumentException("Expected date,time,price,volume");
                Tick t=new Tick(); t.symbol=symbol;
                t.time=LocalDateTime.parse(f[0].trim()+" "+f[1].trim(),INPUT_TIME).atZone(zone).toInstant().toEpochMilli();
                t.price=new BigDecimal(f[2].trim()); t.volume=Long.parseLong(f[3].trim());
                if(t.price.signum()<=0 || t.volume<0) throw new IllegalArgumentException("Price must be positive; volume non-negative");
                if(t.time<previous) throw new IllegalArgumentException("Input timestamps must be non-decreasing");
                previous=t.time; ticks.add(t);
            } catch(RuntimeException e) { throw new IllegalArgumentException("Invalid input row "+row+": "+e.getMessage(),e); }
        }
        if(ticks.isEmpty()) throw new IllegalArgumentException("No records in input");
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        var data=env.fromData(ticks.toArray(Tick[]::new)).name("Stock ticks").uid("stock-input")
            .map(new ReplayPace(delay)).name("Replay pacing")
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Tick>forMonotonousTimestamps()
                .withTimestampAssigner((tick,old)->tick.time));
        var minutes=data.keyBy(t->t.symbol)
            .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
            .aggregate(new Statistics(),new ReportWindow(false,threshold))
            .name("1 minute - price and volume report").uid("minute-report");
        var alerts=data.keyBy(t->t.symbol)
            .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
            .aggregate(new Statistics(),new ReportWindow(true,threshold))
            .name("5 minutes - maximum price alerts").uid("price-alerts");
        Files.createDirectories(output);
        int minuteCount=0,alertCount=0;
        try(CloseableIterator<String> it=minutes.union(alerts).executeAndCollect("Stock Data Analysis");
            var minuteWriter=Files.newBufferedWriter(output.resolve("minute-report.csv"));
            var alertWriter=Files.newBufferedWriter(output.resolve("price-alerts.csv"))) {
            minuteWriter.write("symbol,window_start_utc,window_end_utc,max_price,min_price,max_volume,min_volume,price_change_pct,volume_change_pct,record_count\n");
            alertWriter.write("symbol,window_start_utc,window_end_utc,previous_max_price,current_max_price,price_change_pct,max_price_time_utc\n");
            while(it.hasNext()) {
                String tagged=it.next();
                if(tagged.startsWith("MINUTE,")) {minuteWriter.write(tagged.substring(7)+"\n");minuteCount++;}
                else {alertWriter.write(tagged.substring(6)+"\n");alertCount++;System.out.println(tagged);}
            }
        }
        System.out.printf("Processed %d records; %d minute reports; %d alerts.%nOutput: %s%n",ticks.size(),minuteCount,alertCount,output.toAbsolutePath());
    }

    public static class Tick implements Serializable {
        public String symbol;
        public long time,volume;
        public BigDecimal price;
        public Tick() {}
    }
    public static class ReplayPace implements MapFunction<Tick,Tick> {
        private final long delay;
        public ReplayPace(long delay) {this.delay=delay;}
        @Override public Tick map(Tick t) throws Exception {if(delay>0) Thread.sleep(delay);return t;}
    }
    public static class Stats implements Serializable {
        public BigDecimal maxPrice,minPrice;
        public long maxVolume,minVolume,maxTime,count;
        public Stats() {}
    }
    public static class Statistics implements AggregateFunction<Tick,Stats,Stats> {
        @Override public Stats createAccumulator() {return new Stats();}
        @Override public Stats add(Tick t,Stats a) {
            if(a.count==0) {a.maxPrice=t.price;a.minPrice=t.price;a.maxVolume=t.volume;a.minVolume=t.volume;a.maxTime=t.time;}
            else {
                if(t.price.compareTo(a.maxPrice)>0) {a.maxPrice=t.price;a.maxTime=t.time;}
                else if(t.price.compareTo(a.maxPrice)==0) a.maxTime=Math.min(a.maxTime,t.time);
                a.minPrice=a.minPrice.min(t.price);
                a.maxVolume=Math.max(a.maxVolume,t.volume);a.minVolume=Math.min(a.minVolume,t.volume);
            }
            a.count++;return a;
        }
        @Override public Stats getResult(Stats a) {return a;}
        @Override public Stats merge(Stats a,Stats b) {
            if(a.count==0)return b;if(b.count==0)return a;
            int comparison=b.maxPrice.compareTo(a.maxPrice);
            if(comparison>0) {a.maxPrice=b.maxPrice;a.maxTime=b.maxTime;}
            else if(comparison==0) a.maxTime=Math.min(a.maxTime,b.maxTime);
            a.minPrice=a.minPrice.min(b.minPrice);a.maxVolume=Math.max(a.maxVolume,b.maxVolume);
            a.minVolume=Math.min(a.minVolume,b.minVolume);a.count+=b.count;return a;
        }
    }
    public static class ReportWindow extends ProcessWindowFunction<Stats,String,String,TimeWindow> {
        private final boolean alertsOnly;
        private final BigDecimal threshold;
        public ReportWindow(boolean alertsOnly,BigDecimal threshold) {this.alertsOnly=alertsOnly;this.threshold=threshold;}
        @Override public void process(String key,Context ctx,Iterable<Stats> input,Collector<String> out) throws Exception {
            Stats current=input.iterator().next();
            ValueState<BigDecimal> previousPrice=ctx.globalState().getState(new ValueStateDescriptor<>("previous-price",Types.BIG_DEC));
            ValueState<Long> previousVolume=ctx.globalState().getState(new ValueStateDescriptor<>("previous-volume",Types.LONG));
            ValueState<Long> previousEnd=ctx.globalState().getState(new ValueStateDescriptor<>("previous-window-end",Types.LONG));
            BigDecimal old=previousPrice.value();
            Long end=previousEnd.value();
            boolean adjacent=end!=null && end==ctx.window().getStart();
            String prefix=key+","+Instant.ofEpochMilli(ctx.window().getStart())+","+Instant.ofEpochMilli(ctx.window().getEnd());
            if(!alertsOnly) {
                String priceChange=old==null?"0.00":adjacent?percentage(current.maxPrice,old):"";
                String volumeChange=old==null?"0.00":adjacent?percentage(BigDecimal.valueOf(current.maxVolume),BigDecimal.valueOf(previousVolume.value())):"";
                out.collect("MINUTE,"+prefix+","+current.maxPrice.toPlainString()+","+current.minPrice.toPlainString()
                    +","+current.maxVolume+","+current.minVolume+","+priceChange+","+volumeChange+","+current.count);
            } else if(adjacent && current.maxPrice.subtract(old).abs().multiply(BigDecimal.valueOf(100)).compareTo(old.multiply(threshold))>0) {
                out.collect("ALERT,"+prefix+","+old.toPlainString()+","+current.maxPrice.toPlainString()+","+percentage(current.maxPrice,old)+","+Instant.ofEpochMilli(current.maxTime));
            }
            previousPrice.update(current.maxPrice);previousVolume.update(current.maxVolume);previousEnd.update(ctx.window().getEnd());
        }
        private static String percentage(BigDecimal value,BigDecimal previous) {
            if(previous.signum()==0) return "";
            return value.subtract(previous).multiply(BigDecimal.valueOf(100)).divide(previous,2,RoundingMode.HALF_UP).toPlainString();
        }
    }
}
