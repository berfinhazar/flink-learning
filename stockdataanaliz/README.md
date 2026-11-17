# Hisse Senedi Veri Analizi

Apache Flink ile işlem fiyatı ve hacim verilerinden dakikalık raporlar ve fiyat değişimi alarmları üreten bir uygulama.

## Nasıl çalışır?

Veriler işlem zamanına göre iki ayrı pencerede analiz edilir:

- **1 dakika:** maksimum/minimum fiyat ve hacim, önceki dakikaya göre maksimum fiyat ve hacmin yüzde değişimi.
- **5 dakika:** maksimum fiyatın önceki 5 dakikalık pencereye göre mutlak değişimi %5'i aştığında alarm.

Fiyat ve hacim özetleri pencere içinde hesaplanır. Önceki pencerenin değerleri Flink state içinde tutulur. Pencereler başlangıç dahil, bitiş hariçtir.

## Veri

`data/FUTURES_TRADES.txt` dosyası 33.546 kayıt içerir:

```text
tarih,saat,fiyat,hacim
06/10/2010,08:00:00,106,348746
```

Tarih biçimi `dd/MM/yyyy` şeklindedir. Hisse adı veride bulunmadığından varsayılan sembol `XYZ` kullanılır. Kaynak saat dilimi varsayılan olarak UTC'dir ve değiştirilebilir.

## Çalıştırma

Gereksinimler: Java 17 ve Maven.

Proje klasöründe:

```bash
bash run.sh
```

Farklı veri, çıktı klasörü, saat dilimi, sembol ve alarm eşiğiyle:

```bash
bash run.sh data/FUTURES_TRADES.txt output UTC XYZ 5
```

## Çıktılar

- `output/minute-report.csv`: dakikalık fiyat/hacim raporu.
- `output/price-alerts.csv`: 5 dakikalık fiyat değişimi alarmları.

İlk dakikanın yüzde değişimleri `0.00` olur. Önceki zaman penceresinde veri yoksa değişim alanı boş kalır ve alarm üretilmez. Önceki maksimum hacim sıfırsa hacim değişimi alanı boş kalır. Alarm eşiği yuvarlanmadan karşılaştırılır; tam %5 değişim alarm oluşturmaz.

## Flink arayüzü

Yerel Flink 2.3.0 kümesi için:

```bash
export FLINK_HOME=/path/to/flink-2.3.0
"$FLINK_HOME/bin/start-cluster.sh"
bash run-cluster.sh
```

Küme zaten çalışıyorsa başlatma adımını atlayabilirsin.

Arayüz: [localhost:8081](http://localhost:8081). **Running Jobs → Stock Data Analysis** üzerinden işi izle; tamamlandığında **Completed Jobs** bölümünde görünür. Akışta 1 dakikalık rapor ve 5 dakikalık alarm operatörleri yer alır.

Kayıtları daha yavaş oynatmak için son argümana kayıt başına bekleme süresini (ms) ekleyebilirsin:

```bash
bash run-cluster.sh "$PWD/data/FUTURES_TRADES.txt" "$PWD/output" UTC XYZ 5 1
```

Bu ayarla veri yaklaşık 34 saniyede oynatılır. Bekleme süresi hesaplamaları değiştirmez. CSV dosyaları işi başlatan istemcinin çıktı klasörüne yazılır.

## Doğrulama

Raporları bağımsız hesaplamayla karşılaştır:

```bash
python3 tools/verify.py
```

Sınır durumlarını içeren küçük demo:

```bash
bash run.sh data/demo-trades.txt output/demo
python3 tools/verify.py data/demo-trades.txt output/demo
```
