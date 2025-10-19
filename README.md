
# Dağıtık Log Analitik Pipeline - Bigdata Ödevi

## 📚 Proje Genel Bakış

Bu, baştan sona eksiksiz bir log analitik pipeline'ı kuracağınız uygulamalı bir büyük veri projesidir.

### Mimari

```
[Log Üretici (Sağlandı)]
      │
      ▼
[HDFS / MinIO] ← SİZİN SEÇİMİNİZ (Adım 1)
      │
      ▼
[Apache Spark] ← Toplu Analiz (Adım 2)
      │
      ▼
[HBase] ← Sonuçları Kaydet (Adım 3)
      │
      ▼
[Dashboard] ← Görselleştirme (Adım 4)
```

## 🎯 Öğrenme Hedefleri

- Dağıtık depolama sistemlerini anlamak (HDFS vs MinIO)
- Apache Spark ile toplu işleme uygulamak
- Sütun tabanlı formatlarla çalışmak (Parquet/ORC)
- Optimizasyon teknikleri uygulamak (Bloom Filter, Bitset)
- NoSQL verisi depolamak ve sorgulamak (HBase)
- Veri görselleştirmeleri oluşturmak

## 📋 Ön Koşullar

- Docker & Docker Compose
- Temel Python, SQL ve Docker bilgisi

## 🚀 Başlarken

### Adım 0: Örnek Log Oluştur (Sağlandı)

Gerçekçi uygulama logları üreten bir log üretici script sağladık.

```bash
# Bağımlılıkları yükle yada dev ortamına bağlan dev ortamı gerekli herşeye sahip
scripts\connect_dev_env.ps1   # Windows PowerShell
# veya
./scripts/connect_dev_env.sh  # Linux/MacOS Terminal

pip install -r requirements.txt

# Logları üret
python scripts/log_generator.py
```

Bu işlem, `logs/` dizininde her biri 500.000 log içeren 10 dosya (toplamda yaklaşık 5 milyon log) oluşturur. Kullanacağınız depolama sistemine göre toplam dosya sayısını ve boyutunu ayarlayabilirsiniz. Örneğin, 100 dosya oluşturmak isterseniz `log_generator.py` scriptinde ilgili parametreleri değiştirerek yine toplamda yaklaşık 5 milyon log üretebilirsiniz. Varsayılan sayı ile başlayabilirsiniz, arttırmak istersen daha sonra yapmanı tavsiye ederim.

### Adım 1: Depolama Altyapını Seç ve Kur(SENİN GÖREVİN)

Dağıtık depolama çözümü olarak **HDFS veya MinIO**'dan birini seçmelisin.

### Depolama Sistemi Seçimi & Gerekçesi

#### Seçenekler

- **A: HDFS (Hadoop Dağıtık Dosya Sistemi)**
- **B: MinIO (S3-Uyumlu Nesne Depolama)**


**Örnek:**

| Alan                      | Açıklama                                                                                 |
|---------------------------|-----------------------------------------------------------------------------------------|
| **Seçtiğim Depolama Sistemi** | HDFS                                                                                   |
| **Gerekçem**              | Büyük veri iş yükleri için optimize edilmiş, ölçeklenebilir ve yaygın olarak kullanılıyor.|
| **Kurulum Notları**       | Docker Compose ile kurulumda port çakışmasına dikkat ettim.|
| **Erişilebilirlik Testi** | `docker exec namenode hdfs dfs -ls /logs` komutu ile (yada Namenode UI ile) dosyaların başarıyla yüklendiğini doğruladım.           |

<!-- Kendi seçimini ve gerekçeni yukarıdaki tabloya eklemelisiniz. -->


**Teslimat**: 
1. Seçtiğin depolama sistem için Docker compose dosyası oluştur ve başlat
2. Oluşturulan logları depolamaya yükle
3. Dosyaların erişilebilir olduğunu doğrula
4. Seçimini ve gerekçeni belgele

### Adım 2: Apache Spark Analizi (SENİN GÖREVİN)

Aşağıdaki analizleri PySpark ile uygula:

1. **Performans Metrikleri**
    - Her endpoint için ortalama yanıt süresi
    - 95.yüzdelik yanıt süresi
    - 99.yüzdelik yanıt süresi
    - Servis bazında hata oranları
    - Bölgeye göre istek dağılımı
    - En çok istek yapan kullanıcılar ve IP'ler
    - ...
    - (Listeyi kendi analiz ihtiyaçlarına göre istediğin kadar uzatabilirsin.)

2. **Veri Dönüşümü**
   - JSON loglarını Parquet formata dönüştür
   - JSON loglarını ORC formata dönüştür
   - Dosya boyutlarını ve sorgu performansını karşılaştır

3. **Optimizasyon Teknikleri**
   - user_id aramaları için Bloom Filter uygula
   - IP adresi filtreleme için Bitset oluştur
   - Sorgu performansı iyileştirmelerini karşılaştır

**Teslimat**:
- Spark job scriptleri (`spark/analysis.py`, `spark/convert_formats.py`, `spark/optimization.py`)
- Performans karşılaştırma raporu (`reports/REPORT.md`)
- Optimize veri formatları raporu (`reports/REPORT.md` içerisine eklenmeli)

### Adım 3: HBase Depolama (SENİN GÖREVİN)

Analiz sonuçlarını HBase'e kaydetmek için:

1. **Tablo Tasarımı**
    - Kolay ve hızlı sorgulama için satır anahtarını iyi seç (ör: user_id, tarih)
    - Kolon ailelerini belirle (ör: metrikler, hata oranları)

2. **Veri Yükleme**
    - Spark ile elde edilen analiz sonuçlarını HBase'e aktar
    - Şema değişikliklerini yönet (gerekirse yeni kolon ekle)

**Teslimat:**
- HBase tablo şeması dosyası (`hbase/schema.py` veya `hbase/schema.yaml` olarak, kullandığınız formatı belirtin)
- Veri yükleme scripti (`/hbase/load_data.py`)
- Görselleştirme yapan dashboard scripti, Hbase'den işlenmiş verileri çekip grafiğe döken (`/hbase/view_results.py`)

### Adım 4: Raporlama Dashboard'u (SENİN GÖREVİN)

Sonuçları görselleştirmek için etkileşimli bir dashboard oluştur `Streamlit` kullanarak veya `Matplotlib`/`Seaborn` kullanarak statik görseller üret:

1. **Gerekli Görselleştirmeler**
   - Yanıt süresi trendleri
   - Servis bazında hata oranı
   - Coğrafi dağılım
   - Trafiği en yüksek endpointler


**Teslimat**:
- Streamlit dashboard yada Matplotlib görselleri (`hbase/view_results.py`)

## 📊 Değerlendirme Kriterleri

| Bileşen | Puan | Açıklama |
|-----------|--------|-------------|
| Depolama Seçimi & Kurulum | 15 | Doğru kurulum ve gerekçe |
| Spark Analizi | 30 | Tüm analizlerin doğru uygulanması |
| Optimizasyon Teknikleri | 20 | Bloom Filter & Bitset uygulaması |
| HBase Tasarımı | 10 | Doğru şema ve verimli sorgular |
| Dashboard | 15 | Bilgilendirici |
| Dokümantasyon && Rapor | 10 | Açık ve kapsamlı |
| **Toplam** | **100** | |

## 📁 Proje Yapısı

```
Bigdata-Lecture/
├── README.md
├── requirements.txt
├── docker/
│   ├── docker-compose-hdfs.yml         # SAĞLANDI
│   ├── docker-compose-minio.yml        # SAĞLANDI
│   ├── docker-compose-hbase.yml        # SAĞLANDI
│   ├── docker-compose-dev.yml          # SAĞLANDI
│   └── Dockerfile.dev                  # SAĞLANDI
│
├── scripts/
│   ├── log_generator.py                # SAĞLANDI
│   ├── connect_dev_env.ps1             # SAĞLANDI
│   ├── connect_dev_env.sh              # SAĞLANDI
│   └── upload_to_hdfs.py / upload_to_minio.py  # SENİN GÖREVİN - depolama seçimine bağlı
│
├── spark/
│   ├── analysis.py                     # SENİN GÖREVİN
│   ├── convert_formats.py              # SENİN GÖREVİN
│   └── optimization.py                 # SENİN GÖREVİN
│
├── hbase/
│   ├── schema.py / schema.yaml         # SENİN GÖREVİN
│   ├── load_data.py                    # SENİN GÖREVİN
│   └── view_results.py                 # SENİN GÖREVİN
│
├── reports/
│   └── REPORT.md                       # SENİN GÖREVİN
```

## 🔧 Sağlanan Dosyalar

Başlaman için aşağıdaki dosyalar sağlanmıştır:

1. `scripts/log_generator.py` - Optimize log üretici
2. `requirements.txt` - Python bağımlılıkları
3. Klasör yapısı ve template dosyalar sağlandı
4. Docker Compose dosyaları (HDFS/MinIO/HBase için)
5. Docker ile Dev Ortamı Kurulum Scriptleri

## 💡 İpuçları & En İyi Uygulamalar

### Ödev Raporu

Ödev raporunu `reports/REPORT.md` dosyasında oluşturmalısın. Örnek bir rapor bu dosyada sağlanmıştır; kendi analizlerini, bulgularını ve karşılaştırmalarını bu dosyayı düzenleyerek ekleyebilirsin. Ek kaynaklar, görseller veya grafikler eklemek istersen, ilgili dosyaları `reports/` klasörüne yerleştirebilirsin.

### Depolama Seçimi İçin
- Hangi sistemi neden seçtiğini kısaca yaz
- Kurulum adımlarını ve yaşadığın sorunları açıkça belirt
- Dosyaların depoya yüklendiğini nasıl test ettiğini yaz

### Spark İşleri İçin
- Kodunu küçük parçalara böl ve her adımı yorum satırıyla açıkla
- Analiz sonuçlarını tablo veya grafik olarak kaydet
- Hatalarla karşılaşırsan hata mesajını ve çözümünü yaz

### HBase Tasarımı İçin
- Satır anahtarını (ör: user_id, tarih) neden öyle seçtiğini yaz
- Kolon ailelerini ve ne işe yaradıklarını açıkla
- Veri yüklerken kullandığın adımları sırayla yaz

### Dashboard İçin
- Kullanıcıya hangi verileri gösterdiğini açıkça belirt
- Kodun nasıl çalıştığını kısaca anlat (yorum satırları ekle)


## 📚 Faydalı Kaynaklar

- [Docker Resmi Dokümantasyonu](https://docs.docker.com/)
- [Apache Spark Dokümantasyonu](https://spark.apache.org/docs/latest/)
- [HBase Dokümantasyonu](https://hbase.apache.org/book.html)
- [MinIO Dokümantasyonu](https://min.io/docs/)
- [HDFS Mimarisi Rehberi](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- [Streamlit Dokümantasyonu](https://docs.streamlit.io/)
- [Matplotlib Dokümantasyonu](https://matplotlib.org/stable/contents.html)

## ❓ SSS

**S: Hem HDFS hem MinIO kullanabilir miyim?**  
C: Ödev için yalnızca bir depolama sistemini seçmelisin. Ancak, her iki sistemi de kurup karşılaştırırsan raporda bonus puan alabilirsin. İkisini aynı anda çalıştırmak port çakışmalarına yol açabilir; bu nedenle birini durdurup diğerini başlatabilirsin.

**S: Her şeyi kendi bilgisayarımda çalıştıramazsam ne olur?**  
C: Bulut kaynaklarını kullan (GitHub Codespaces kullanabilirsin) veya veri boyutunu küçült.

**S: Farklı görselleştirme araçları kullanabilir miyim?**  
C: Evet, ancak basitlik için Streamlit veya Matplotlib önerilir.

**S: Nasıl teslim edeceğim?**  
C: Github Classroom üzerinden teslim et (oluşturduğun takıma arkadaşlarının da katıldığına emin ol).

## 🏆 Bonus Puanlar

- Ekstra analizler ekle - **+10 puan**
- Hem HDFS hem MinIO ile karşılaştırma - **+10 puan**
- Gelişmiş Dashboard özellikleri - **+10 puan**
- Sizin aklınıza gelen diğer iyileştirmeleri ekleyin ve raporlayın - **+∞ puan**

## 📅 Son Teslim Tarihi

**Teslim Tarihi**: [İLAN EDİLECEK]

## 👥 Takım Bilgisi

- **Takım Büyüklüğü**: 4 öğrenci
- **İşbirliği**: Sürüm kontrolü için GitHub kullanın
- **İletişim**: Tasarım kararlarınızı belgeleyin

## 📧 Destek

Sorularınız varsa:
1. SSS bölümünü kontrol edin
2. Sağlanan dokümantasyonu inceleyin
3. Ödev ile ilgili sorularınızı ve yaşadığınız sorunları bu repoda issue olarak açın eğitmenler yada öğrenciler birbirine yardımcı olabilir:
    - [Repo](https://github.com/IsmailTosunTnyl/Bigdata-Lecture-Project/issues)
4. Eğitmene ulaşın: 
    - [İsmail Tosun - LinkedIn](https://www.linkedin.com/in/ismailtosundev)
    - [İsmail Tosun - Email](mailto:ismailtosuntny@gmail.com)

---

**Başarılar! 🚀**

Unutmayın: Amaç sadece görevleri tamamlamak değil, tüm veri boru hattını anlamaktır. Öğrenmeye odaklanın!



---

## 🔢 Proje Adımları (Özet)

### 1. Geliştirme Ortamını Başlat
- Tüm Python scriptlerini dev ortamında çalıştırmalısın. Gerekli bağımlılıklar orada yüklü.
- Docker Compose dosyalarını **host makinede** başlatacaksın, dev ortamında değil!
- Dev ortamına bağlanınca özel bir prompt göreceksin:
    ```bash
    [DEV-ORTAM] user@hostname:/workspace#
    ```
- Bağlanmak için:
    - **Windows PowerShell:**
        ```powershell
        scripts\start_dev_env.ps1
        ```
    - **Linux/MacOS Terminal:**
        ```bash
        ./scripts/start_dev_env.sh
        ```

### 2. Logları Oluştur (Log Generator)
- Dev ortamında çalıştır:
    ```bash
    python scripts/log_generator.py
    ```

### 3. Depolama Sistemini Seç ve Başlat (HDFS veya MinIO)
- Host makinede başlat:
    ```bash
    # HDFS için
    docker-compose -f docker/docker-compose-hdfs.yml up -d
    # MinIO için
    docker-compose -f docker/docker-compose-minio.yml up -d
    ```

### 4. Logları Depolama Sistemine Yükle
- Dev ortamında çalıştır:
    ```bash
    python scripts/upload_to_storage.py
    ```

### 5. Spark Analizlerini Çalıştır
- Dev ortamında çalıştır:
    ```bash
    # Veri formatlarını dönüştür ve dosya boyutlarını karşılaştır
    python spark/convert_formats.py
    # Analizleri çalıştır (raw log veya parquet/orc dosyası ile)
    python spark/analysis.py
    python spark/optimization.py
    ```

### 6. Analiz Sonuçlarını HBase'e Yükle
- Dev ortamında çalıştır:
    ```bash
    python hbase/load_data.py
    ```

### 7. Dashboard'u Çalıştır ve Sonuçları Görselleştir
- Dev ortamında çalıştır:
    ```bash
    streamlit run hbase/view_results.py --server.address 0.0.0.0 --server.port 8501
    ```

