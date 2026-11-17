# Twitter veri analizi — Flink 2.3.0

Bu bağımsız Maven projesi, kurstaki Twitter analizini yerel JSONL verisiyle gerçekleştirir. Canlı Twitter bağlantısı veya API anahtarı gerekmez. Java 17 ve Maven kullanır.

## Veri nereden geliyor?

Paylaşılan `Twitter Analysis_flink_V_1.13` klasöründe yalnızca `Twitter_UseCase.java` ve `pom.xml` var; tweet veri dosyası yok. Orijinal program boş erişim bilgileriyle TwitterSource üzerinden canlı veri bekliyor.

`data/tweets-sample.jsonl` bu proje için hazırlanmış 16 **sentetik** kayıttır; gerçek tweet veya pazar araştırması değildir. Her satır bir JSON nesnesidir. Dil, metin, kaynak uygulama ve gönderim zamanını içerir. 13 kayıt filtreleri geçer; Türkçe bir kayıt ve ilgisiz iki kayıt elenir.

## Çalıştırma

Proje klasöründeyken:

```bash
bash run.sh
```

Script macOS üzerinde Java 17 kurulumunu otomatik bulur. Başka sistemlerde Java 17 PATH içinde olmalı veya `JAVA_HOME` tanımlanmalıdır.

İsteğe bağlı girdi, çıktı ve saat dilimi:

```bash
bash run.sh data/tweets-sample.jsonl output/istanbul.csv Europe/Istanbul
```

Varsayılan saat dilimi UTC. Sonuçlar terminale ve `output/counts.csv` dosyasına yazılır. Aynı çıktı yolu tekrar kullanılırsa dosya yenilenir. Bu küçük dosya uygulamanın belleğine alınarak Flink'e aktarılır; büyük veya kesintisiz kaynaklar için tasarlanmamıştır.

## İşlem zinciri

JSON satırları → İngilizce tweetler → çevre anahtar kelimeleri → cihaz ve tarih/saat → keyBy(cihaz, saat) → sum(2).

Kodun başlangıcı: `src/main/java/com/berfin/flink/TwitterDataAnalysis.java`.

`flatMap`: JSON ayrıştırır, İngilizce olmayan ve anahtar kelime içermeyen kayıtları eler, `(cihaz, saat, 1)` üretir. `keyBy` aynı cihaz ve saati birleştirir. `sum(2)` üçüncü alanı state içinde biriktirir. `executeAndCollect` küçük örneğin sonuçlarını CSV'ye aktarır.

Çıktı **artan sayaç güncellemeleridir**. Örneğin aynı grup için önce 1, sonra 2 yazılır; bunları toplayarak 3 sonucu çıkarmamalısın. Her grubun son satırı o grubun son sayısıdır.

## Beklenen son sayılar (UTC)

| Tarih/saat | Cihaz | Tweet sayısı |
|---|---|---:|
| 2026-11-17 10:00 | AppleMobile | 2 |
| 2026-11-17 10:00 | Android | 2 |
| 2026-11-17 10:00 | Web | 1 |
| 2026-11-17 10:00 | AppleMac | 1 |
| 2026-11-17 10:00 | BlackBerry | 1 |
| 2026-11-17 10:00 | WindowsPhone | 1 |
| 2026-11-17 10:00 | Other | 2 |
| 2026-11-17 11:00 | AppleMobile | 1 |
| 2026-11-17 11:00 | Android | 1 |
| 2026-11-18 10:00 | AppleMobile | 1 |

## Kurstaki koddan farklar

- Eski TwitterSource yerine sonlu bir dosya tekrar oynatılır; bu çalıştırma canlı Twitter akışı değildir.
- Cihazla birlikte tarih de gruplanır; farklı günlerin aynı saatleri karışmaz. Bu bir zaman penceresi değildir, tarih/saat anahtarı üzerinde birikimli sayımdır.
- Dil için önce tweet'in `lang` alanı kullanılır; yoksa eski örnekteki `user.lang` alanına bakılır.
- BlackBerry kontrolündeki büyük/küçük harf hatası düzeltilmiştir. Kaynağı eksik veya bilinmeyen tweetler Other olur. HTML bağlantısının görünen uygulama adı değerlendirilir.
- ISO zamanları ve dersin eski `Thu May 10 15:24:15 +0000 2018` biçimi okunabilir.
- Geçersiz JSON veya ilgili bir kayıtta geçersiz tarih varsa iş hata verir; veriyi düzeltip yeniden çalıştır.

Sayım benzersiz kullanıcıları değil tweetleri ölçer. Kaynak uygulama etiketi cihaz için bir yaklaşımdır. Anahtar kelime eşleştirmesi basit alt dize aramasıdır; duygu analizi yapmaz. Bu veriler uygulama indirme niyetini veya gerçek platform pazar payını kanıtlamaz.
