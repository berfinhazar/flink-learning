# Banka İşlemlerinde Dolandırıcılık Analizi

Apache Flink ile banka işlemlerini inceleyen ve şüpheli hareketler için nedenleriyle birlikte alarm üreten bir uygulama. Yerel işlem dosyasını zaman sırasıyla işler.

## Nasıl çalışır?

İşlemler müşteri kimliğine göre gruplanır. Flink state ile her müşterinin son 60 saniyedeki işlemleri ve işlem yaptığı şehirler takip edilir.

| Kural | Alarm koşulu |
|---|---|
| Alarmlı müşteri | Müşteri takip listesinde bulunuyorsa |
| Kayıp kart | Kullanılan kart kayıp kart listesinde bulunuyorsa |
| İşlem yoğunluğu | Son 60 saniyede 10'dan fazla işlem varsa |
| Farklı şehirler | Son 60 saniyede 2'den fazla farklı şehirde işlem varsa |

Bir işlem birden fazla kuralı tetikleyebilir. Her alarm nedeni ayrı çıktı satırıdır. Kart numaraları çıktıda maskelenir.

## Veriler

`data/course/` içinde işlem verisi (`bank_data.txt`), alarmlı müşteriler (`alarmed_cust.txt`) ve kayıp kartlar (`lost_cards.txt`) bulunur. 53 işlemden 8 alarm üretilir.

`data/demo/` dört kuralı da örnekleyen 29 sentetik işlem içerir ve 5 alarm üretir.

## Çalıştırma

Gereksinimler: Java 17 ve Maven.

Proje klasöründe:

```bash
bash run.sh
```

Demo verisiyle:

```bash
bash run.sh data/demo output/demo-alerts.csv
```

Sonuçlar terminale ve `output/course-alerts.csv` dosyasına yazılır. Kaynak saat dilimi varsayılan olarak `Asia/Kolkata`, çıktı saat dilimi UTC'dir.

## Flink arayüzü

Yerel Flink 2.3.0 kümesinde çalıştırmak için:

```bash
export FLINK_HOME=/path/to/flink-2.3.0
"$FLINK_HOME/bin/start-cluster.sh"
bash run-cluster.sh
```

Flink kümesi zaten çalışıyorsa başlatma adımını atlayabilirsin.

Arayüz: [localhost:8081](http://localhost:8081). **Running Jobs → Bank Fraud Analysis** üzerinden işi izleyebilirsin. Tamamlandığında **Completed Jobs** bölümünde görünür.

Daha yavaş oynatmak için kayıtlar arasındaki beklemeyi 1000 ms olarak ayarlayabilirsin:

```bash
bash run-cluster.sh "$PWD/data/course" "$PWD/output/course-alerts.csv" Asia/Kolkata 1000
```

## Sonuçları doğrulama

```bash
python3 tools/verify.py
```

Demo çalıştırıldıktan sonra:

```bash
python3 tools/verify.py data/demo output/demo-alerts.csv
```
