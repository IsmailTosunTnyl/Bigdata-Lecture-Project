"""
JSON'dan Parquet/ORC'ye Dönüştürme - ÖDEV ŞABLONU
==================================================
GÖREV: JSON log dosyalarını Parquet ve ORC formatlarına dönüştüren bir Spark uygulaması yazın.

ÖĞRENİLECEKLER:
- Spark Session yapılandırması (MinIO/HDFS için)
- JSON dosyalarını şema ile okuma
- Parquet ve ORC formatlarına yazma
- Sıkıştırma codec'leri kullanma
- Performans karşılaştırması yapma

TAMAMLANMASI GEREKEN GÖREVLER:
1. create_spark_session() - MinIO veya HDFS için Spark Session yapılandırın
2. load_json_logs() - JSON dosyalarını şema ile okuyun
3. convert_to_parquet() - DataFrame'i Parquet formatına yazın
4. convert_to_orc() - DataFrame'i ORC formatına yazın
5. compare_formats() - Formatların performansını karşılaştırın

İPUCLARI:
- Parquet: Kolon bazlı format, analitik için ideal (.parquet())
- ORC: ACID destekli, yüksek sıkıştırma (.orc())
- Sıkıştırma codec'leri: snappy, gzip, zlib vb.
- spark.read.schema() ile şema tanımlayın
- .cache() ile DataFrame'i bellekte tutun
- time.time() ile performansı ölçün

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, to_timestamp
import sys
import time
from datetime import datetime

# ====================================
# YAPABDIRILMIŞ KISIM - Yapılandırma
# ====================================
class Config:
    """Dönüştürme görevi için yapılandırma sınıfı - HAZIR"""
    
    # Depolama Yapılandırması (YALNIZCA BİRİNİ seçin)
    STORAGE_TYPE = "hdfs"  # Seçenekler: "minio" veya "hdfs"
    
    # MinIO Yapılandırması
    MINIO_ENDPOINT = "http://minio:9000"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin123"
    MINIO_INPUT_PATH = "s3a://logs/raw/*.json"
    MINIO_PARQUET_OUTPUT = "s3a://parquet/logs"
    MINIO_ORC_OUTPUT = "s3a://orc/logs"
    
    # HDFS Yapılandırması
    HDFS_INPUT_PATH = "hdfs://namenode:9000/logs/raw/*.json"
    HDFS_PARQUET_OUTPUT = "hdfs://namenode:9000/logs/parquet"
    HDFS_ORC_OUTPUT = "hdfs://namenode:9000/logs/orc"
    
    # Spark Yapılandırması
    APP_NAME = "JSON_to_Parquet_ORC_Converter"
    EXECUTOR_MEMORY = "3g"
    DRIVER_MEMORY = "2g"
    
    # Sıkıştırma ayarları
    PARQUET_COMPRESSION = "snappy"  # Seçenekler: snappy, gzip, lzo, brotli, lz4, zstd
    ORC_COMPRESSION = "snappy"      # Seçenekler: snappy, zlib, lzo, lz4, none


# ====================================
# GÖREV 1: Spark Session Oluşturma
# ====================================
def create_spark_session(config):
    """
    Depolama tipine göre Spark oturumu oluştur ve yapılandır
    
    YAPILACAKLAR:
    1. SparkSession.builder ile session başlatın
    2. config'den APP_NAME, EXECUTOR_MEMORY, DRIVER_MEMORY ayarlayın
    3. Parquet ve ORC compression codec'lerini ayarlayın
    4. STORAGE_TYPE'a göre MinIO veya HDFS yapılandırması ekleyin
    5. Session'ı oluşturun ve log seviyesini "WARN" yapın
    
    MinIO için gerekli config'ler:
    - spark.jars.packages: "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
    - spark.hadoop.fs.s3a.endpoint
    - spark.hadoop.fs.s3a.access.key
    - spark.hadoop.fs.s3a.secret.key
    - spark.hadoop.fs.s3a.path.style.access: "true"
    - spark.hadoop.fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
    - spark.hadoop.fs.s3a.connection.ssl.enabled: "false"
    
    HDFS için gerekli config:
    - spark.hadoop.dfs.client.use.datanode.hostname: "true"
    
    Args:
        config: Config nesnesi
    Returns:
        SparkSession: Yapılandırılmış Spark oturumu
    """
    # TODO: Spark Session'ı yapılandırın ve oluşturun
    pass


# ====================================
# GÖREV 2: JSON Verilerini Yükleme
# ====================================
def load_json_logs(spark, config):
    """
    JSON loglarını şema ile okuyun ve DataFrame döndürün
    
    YAPILACAKLAR:
    1. STORAGE_TYPE'a göre doğru input_path'i seçin
    2. Log şemasını StructType ile tanımlayın (timestamp, service, endpoint, level, user_id, ip, region, response_time, status_code, error_code, message)
    3. spark.read.schema().json() ile JSON dosyalarını okuyun
    4. timestamp kolonunu to_timestamp() ile datetime'a çevirin
    5. df.cache() ile DataFrame'i bellekte tutun
    6. Yükleme süresini ve kayıt sayısını yazdırın
    7. df.show() ile örnek veri gösterin
    
    Schema örneği:
        StructType([
            StructField("timestamp", StringType(), True),
            StructField("service", StringType(), True),
            ...
        ])
    
    Args:
        spark: SparkSession nesnesi
        config: Config nesnesi
    Returns:
        DataFrame: JSON loglarını içeren DataFrame
    """
    print("\n" + "="*60)
    print("📥 JSON LOG'LAR YÜKLENİYOR")
    print("="*60)
    
    # TODO: Input path'i belirleyin (MINIO veya HDFS)
    
    # TODO: Şema tanımlayın
    
    # TODO: JSON dosyalarını okuyun
    
    # TODO: timestamp kolonunu datetime'a çevirin
    
    # TODO: DataFrame'i cache'leyin
    
    # TODO: Performans metriklerini yazdırın
    
    pass


# ====================================
# GÖREV 3: Parquet'e Dönüştürme
# ====================================
def convert_to_parquet(spark, df, config):
    """
    DataFrame'i Parquet formatına yazın
    
    YAPILACAKLAR:
    1. STORAGE_TYPE'a göre output path'i belirleyin
    2. df.write.mode("overwrite").parquet(output_path) ile yazın
    3. Yazma süresini ölçün ve yazdırın
    4. spark.read.parquet() ile doğrulama yapın
    5. Toplam kayıt sayısını ve partition sayısını yazdırın
    
    İpucu: Sıkıştırma codec'i zaten Spark Session'da ayarlandı
    
    Args:
        spark: SparkSession nesnesi
        df: Dönüştürülecek DataFrame
        config: Config nesnesi
    """
    print("\n" + "="*60)
    print("📦 PARQUET FORMATINA DÖNÜŞTÜRÜLÜYOR")
    print("="*60)
    
    # TODO: Output path'i belirleyin
    
    # TODO: Parquet formatında yazın
    
    # TODO: Yazma performansını yazdırın
    
    # TODO: Doğrulama yapın
    
    pass


# ====================================
# GÖREV 4: ORC'ye Dönüştürme
# ====================================
def convert_to_orc(spark, df, config):
    """
    DataFrame'i ORC formatına yazın
    
    YAPILACAKLAR:
    1. STORAGE_TYPE'a göre output path'i belirleyin
    2. df.write.mode("overwrite").orc(output_path) ile yazın
    3. Yazma süresini ölçün ve yazdırın
    4. spark.read.orc() ile doğrulama yapın
    5. Toplam kayıt sayısını ve partition sayısını yazdırın
    
    İpucu: ORC, Parquet'e benzer ama farklı sıkıştırma kullanır
    
    Args:
        spark: SparkSession nesnesi
        df: Dönüştürülecek DataFrame
        config: Config nesnesi
    """
    print("\n" + "="*60)
    print("📦 ORC FORMATINA DÖNÜŞTÜRÜLÜYOR")
    print("="*60)
    
    # TODO: Output path'i belirleyin
    
    # TODO: ORC formatında yazın
    
    # TODO: Yazma performansını yazdırın
    
    # TODO: Doğrulama yapın
    
    pass


# ====================================
# GÖREV 5: Format Karşılaştırması
# ====================================
def compare_formats(spark, config, original_count):
    """
    JSON, Parquet ve ORC formatlarının okuma performansını karşılaştırın
    
    YAPILACAKLAR:
    1. Her format için dosya yolunu belirleyin
    2. Her formatı okuyun ve okuma süresini ölçün
    3. Kayıt sayısını doğrulayın
    4. Okuma hızlarını karşılaştırın (kayıt/saniye)
    5. Sonuçları tablo formatında yazdırın
    
    İpucu: time.time() ile başlangıç ve bitiş zamanını ölçün
    
    Args:
        spark: SparkSession nesnesi
        config: Config nesnesi
        original_count: Orijinal JSON'daki kayıt sayısı
    """
    print("\n" + "="*60)
    print("📊 FORMAT KARŞILAŞTIRMASI")
    print("="*60)
    
    # TODO: Her format için path'leri tanımlayın
    
    # TODO: Her formatı okuyun ve performansı ölçün
    
    # TODO: Sonuçları karşılaştırın
    
    pass


# ====================================
# Yardımcı Fonksiyon - HAZIR
# ====================================
def generate_summary(total_records, start_time, config):
    """
    Özet istatistikleri yazdır - HAZIR
    """
    print("\n" + "="*60)
    print("📋 DÖNÜŞTÜRME ÖZETİ")
    print("="*60)
    
    execution_time = time.time() - start_time
    
    print(f"\n✅ Dönüştürme Tamamlandı!")
    print(f"   Toplam Kayıt: {total_records:,}")
    print(f"   Toplam Çalışma Süresi: {execution_time:.2f} saniye")
    print(f"   Ortalama İşleme Hızı: {total_records/execution_time:,.0f} kayıt/saniye")
    
    print(f"\n📁 Çıktı Konumları:")
    if config.STORAGE_TYPE == "minio":
        print(f"   Parquet: {config.MINIO_PARQUET_OUTPUT}")
        print(f"   ORC: {config.MINIO_ORC_OUTPUT}")
    else:
        print(f"   Parquet: {config.HDFS_PARQUET_OUTPUT}")
        print(f"   ORC: {config.HDFS_ORC_OUTPUT}")
    
    print(f"\n💡 Sıkıştırma Ayarları:")
    print(f"   Parquet: {config.PARQUET_COMPRESSION}")
    print(f"   ORC: {config.ORC_COMPRESSION}")


# ====================================
# Ana Çalıştırma - TAMAMLAYIN
# ====================================
def main():
    """
    Ana çalıştırma fonksiyonu - TODO'ları tamamlayın
    """
    print("\n" + "="*60)
    print("🚀 JSON'DAN PARQUET/ORC'YE DÖNÜŞTÜRME - ÖDEV")
    print("="*60)
    print(f"Başlangıç Zamanı: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")

    start_time = time.time()
    config = Config()

    try:
        # TODO: GÖREV 1 - Spark Session oluşturun
        # spark = create_spark_session(config)
        # print(f"\n✅ Spark Oturumu Oluşturuldu")

        # TODO: GÖREV 2 - JSON verilerini yükleyin
        # df = load_json_logs(spark, config)
        # original_count = df.count()

        # TODO: GÖREV 3 - Parquet'e dönüştürün
        # convert_to_parquet(spark, df, config)

        # TODO: GÖREV 4 - ORC'ye dönüştürün
        # convert_to_orc(spark, df, config)

        # TODO: GÖREV 5 - Formatları karşılaştırın
        # compare_formats(spark, config, original_count)

        # TODO: Özet yazdırın (uncomment edin)
        # generate_summary(original_count, start_time, config)

        print("\n" + "="*60)
        print("✅ TÜM DÖNÜŞTÜRMELER BAŞARIYLA TAMAMLANDI")
        print("="*60)

    except Exception as e:
        print(f"\n❌ Dönüştürme sırasında hata: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        # TODO: Spark session'ı durdurun (uncomment edin)
        # spark.stop()
        print(f"\n🛑 İşlem tamamlandı")


if __name__ == "__main__":
    main()
