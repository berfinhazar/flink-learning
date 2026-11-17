# Banka işlemlerinde şüpheli hareket analizi

Java 17 / Flink 2.3.0 ile bağımsız Maven projesi. Dört kurala göre inceleme alarmları üretir; banka işlemlerini bloke etmez veya kesin dolandırıcılık kararı vermez.

## Veriler

Hedef klasördeki `.textClipping` dosyası yalnızca `bank_flink_V_1.13` metnini içeriyordu. Dersin gerçek kaynak klasörü Downloads altında bulundu. Üç ders veri dosyası içerikleri korunup satır sonları LF biçimine dönüştürülerek `data/course/` içine kopyalandı:

- `bank_data.txt`: 53 işlem, 7 sütun: işlem kimliği, yerel tarih/saat, şehir, müşteri kimliği, hesap, kart numarası, tutar. Bu sıra kaynak kodla doğrulandı; anlatımdaki hesap/kart sırası farklı anlaşılabilir.
- `alarmed_cust.txt`: müşteri kimliği, hesap.
- `lost_cards.txt`: kart numarası, bildirim tarihi, bildiren kişi, durum.

Bunlar kursun sağladığı verilerdir; gerçek banka kayıtları oldukları doğrulanmamıştır. `data/demo/` dosyaları ise bu proje için üretilmiş açıkça DEMO olarak adlandırılmış test kayıtlarıdır.

## Çalıştırma

```bash
cd /path/to/flink-learning/bankarealdataanaliz
bash run.sh
```

Demo için:

```bash
bash run.sh data/demo output/demo-alerts.csv
```

İsteğe bağlı konumsal argümanlar: veri klasörü, çıktı dosyası, kaynak saat dilimi, kayıt başına bekleme (milisaniye). Kaynak tarihlerinde saat dilimi yok; şehirler Hindistan'da olduğu için varsayım `Asia/Kolkata`. Çıktı zamanı UTC olarak yazılır. Örneğin:

```bash
bash run.sh data/course output/course-alerts.csv Asia/Kolkata 300
```

CSV yeniden çalıştırmada aynı yolda yenilenir. Kaynak dosyalar çalıştırma sırasında değişmez.

## Flink arayüzünde izleme

Mevcut yerel Flink kümesi çalışırken:

```bash
cd /path/to/flink-learning/bankarealdataanaliz
export FLINK_HOME=/path/to/flink-2.3.0
bash run-cluster.sh
```

Önce `export FLINK_HOME=/path/to/flink-2.3.0` ile kurulum klasörünü belirt. Java için JAVA_HOME geçerlidir; macOS üzerinde Java 17 otomatik bulunur, diğer sistemlerde Java 17 PATH içinde olmalıdır.

http://localhost:8081 altında Running Jobs → Bank Fraud Analysis. Cluster scripti varsayılan olarak kayıt başına 300 ms bekler; yaklaşık 16 saniyelik tekrar oynatmayı izleyebilirsin. İş bittiğinde Completed Jobs'a geçer. Daha uzun izlemek için:

```bash
bash run-cluster.sh "$PWD/data/course" "$PWD/output/course-alerts.csv" Asia/Kolkata 1000
```

İsimlendirilmiş operatörler kaynak, oynatma, zaman damgası ve dört kuralı gösterir. Tek işi dört kural işlediği için ayrı dört alarm operatörü yoktur. Sonuç satırları istemci terminaline ve CSV'ye gelir, arayüz sonuç tablosu değildir. Script, derleme ve gönderimi yapar; küme başlatmaz. Gerekirse kurulumun `bin/start-cluster.sh` betiğiyle kümeyi başlat.

Bu komut aynı bilgisayarda çalışan bir session cluster içindir. İstemci veriyi yerel dosyadan okuyup Flink'e gönderir ve `executeAndCollect` ile çıktıyı toplar; dağıtık bir ortam için sürekli kaynak ve kalıcı sink tasarımı gerekir. JAR, küme gönderimi için `target/bankarealdataanaliz-1.0-SNAPSHOT.jar` dosyasıdır.

## Kurallar ve state

| Alarm | Koşul |
|---|---|
| ALARMED_CUSTOMER | Müşteri kimliği alarmlı müşteri listesinde |
| LOST_CARD | Kart numarası kayıp kart listesinde |
| MORE_THAN_10_TRANSACTIONS_60S | Aynı müşterinin son 60 saniyede 10'dan fazla işlemi |
| MORE_THAN_2_CITIES_60S | Aynı müşterinin son 60 saniyede 2'den fazla farklı şehirde işlemi |

Her işlem tüm kurallardan geçer. Bir işlem için birden fazla neden varsa her neden ayrı CSV satırıdır. `input_row` kaynak satırı belirtir; kaynakta yinelenen işlem kimlikleri bulunduğu için kayıtları ayırt etmeyi sağlar. Tekrarlar otomatik silinmez: dosyanın her satırı bir olaydır.

Müşteri bazında `keyBy` uygulanır. Flink `ListState` her müşteri için son 60 saniyenin zaman/şehir kayıtlarını tutar. Hem yeni işlem geldiğinde hem event-time timer ile eski state temizlenir. Aralık `(t - 60 saniye, t]`: tam 60 saniye önceki işlem hariçtir. Alarm 11'inci ve eşiği aşmaya devam eden sonraki işlemlerde üretilir. Şehir karşılaştırması büyük/küçük harfe duyarsızdır; Delhi → Mumbai → Delhi iki şehir sayılır ve şehir alarmı oluşturmaz.

## Ders kodundan bilinçli farklar

- Dersin anlatımı bir dakika diyor; sağlanan kodda işlem sayımı için 10 saniyelik processing-time penceresi var. Burada kaynak zamanlarına göre hareketli 60 saniye kullanılır. Dosyayı hızlı oynatmak yapay işlem yoğunluğu yaratmaz.
- Veriler dosyada zaman sırasından çıkabiliyor. Sonlu dosya zaman ve kaynak satırı sırasına göre kararlı biçimde sıralanır. Bu nedenle monoton watermark kullanılabilir. Canlı ve sırasız kaynaklara aynı watermark varsayımı uygulanmamalıdır.
- Kaynak şehir değişimlerini sayıyor; burada kullanıcı isteğindeki üç farklı şehir kontrol ediliyor.
- Referans listeleri başlangıçta yüklenen sabit snapshot olarak her görev kopyasına gönderilir. Bu sürüm dinamik Broadcast State güncellemesi yapmaz. Böylece küçük sonlu örnekte referans kayıtları yüklenmeden işlem kontrol edilmesi yarışı oluşmaz. Liste değişikliği için işi yeniden başlat. Dinamik listeler ayrı bir broadcast protokolü ve başlangıç senkronizasyonu gerektirir.
- Kart numaraları çıktı ve terminalde maskelenir, hesap numarası yazılmaz. Verilen kaynak dosyalar aynen korunur.

Bu bir sonlu veriyle çalışan eğitim uygulamasıdır. Canlı banka entegrasyonu, restore/checkpoint işletimi, işlem kimliğiyle deduplikasyon, geç gelen kayıt politikası ve exactly-once dış çıktı garantisi sunmaz. Belleğe tüm dosyayı alır, yakın geçmişteki olayları tarar; yüksek hacimli üretim için optimize edilmemiştir.

## Beklenen sonuçlar ve doğrulama

Ders verisi: **53 kayıt, 8 alarm satırı** (6 alarmlı müşteri, 2 kayıp kart). Son 60 saniye için diğer iki eşik bu dosyada aşılmaz; alarm çıkmaması veriyle uyumludur.

Demo: dört kuralın tamamını, aynı işlem için iki nedeni, iki şehir arasında geri dönüşü, state süresinin dolmasını ve tam 60 saniye sınırını kapsar. Demo verisinde 29 kayıt ve 5 alarm satırı beklenir.

```bash
python3 tools/verify.py
python3 tools/verify.py data/demo output/demo-alerts.csv
```

Doğrulayıcı Flink'ten bağımsız bir toplu referans hesaplamasıyla tüm alarm satırlarını, sayıları, zamanları ve kaynak satırlarını karşılaştırır. Sonuç CSV'leri `output/` altındadır.
