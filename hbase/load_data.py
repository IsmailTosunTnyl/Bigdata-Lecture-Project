"""
Spark Analiz Sonuçlarını HBase'e Yükle
=======================================
Bu betik, Spark analizinden çıkan sonuçları (HDFS/Parquet'te saklanan)
HBase tablolarına yükler. Böylece hızlı sorgulama ve gerçek zamanlı erişim sağlanır.
"""

import happybase
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
import time

# ====================================
# Configuration
# ====================================
class Config:
    """HBase'e yükleme için yapılandırma sınıfı"""
    
    # Depolama Yapılandırması (YALNIZCA BİRİNİ seçin)
    STORAGE_TYPE = "hdfs"  # Seçenekler: "minio" veya "hdfs"
    
    # HBase Ayarları
    HBASE_HOST = "hbase"
    HBASE_PORT = 9090
    
    # MinIO Ayarları
    MINIO_ENDPOINT = "http://minio:9000"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin123"
    MINIO_RESULTS_PATH = "s3a://analytics/results"
    
    # HDFS Ayarları
    HDFS_RESULTS_PATH = "hdfs://namenode:9000/analytics/results"
    
    # HBase Tablo Tanımları ihtiyacına göre Değiştirebilirsin
    TABLES = {
        "response_metrics": {
            "cf": ["metrics"],
            "columns": ["endpoint", "request_count", "avg_response_time", 
                       "median_response_time", "p95_response_time", "p99_response_time"]
        },
        "service_errors": {
            "cf": ["stats"],
            "columns": ["service", "total_requests", "error_count", 
                       "warn_count", "error_rate", "warn_rate"]
        },
        "region_traffic": {
            "cf": ["traffic"],
            "columns": ["region", "request_count", "avg_response_time"]
        },
        "hourly_traffic": {
            "cf": ["traffic"],
            "columns": ["hour", "request_count", "avg_response_time"]
        },
        "top_users": {
            "cf": ["user"],
            "columns": ["user_id", "request_count"]
        }
    }

# ====================================
# HBase Functions
# ====================================
def connect_to_hbase(config):
    """Thrift ile HBase'e bağlan"""
    print(f"\n{'='*60}")
    print(f"📡 HBase'e bağlanılıyor: {config.HBASE_HOST}:{config.HBASE_PORT}")
    print(f"{'='*60}")
    
    try:
        connection = happybase.Connection(
            host=config.HBASE_HOST,
            port=config.HBASE_PORT,
            timeout=30000
        )
        print("✅ HBase bağlantısı başarılı")
        return connection
    except Exception as e:
        print(f"❌ HBase bağlantı hatası Docker Containerlarını kontrol edin: {e}")
        sys.exit(1)

def create_tables(connection, config):
    """HBase tabloları yoksa oluştur"""
    print(f"\n{'='*60}")
    print("🔧 HBase Tabloları Oluşturuluyor")
    print(f"{'='*60}")
    
    # TODO 2: HBase tablolarını oluştur
    # İpucu: connection.tables() ile mevcut tabloları listele
    # İpucu: Her tablo için config.TABLES sözlüğünü döngüyle gez
    # İpucu: Tablo varsa atla, yoksa families = {cf: dict() for cf in table_def["cf"]} ile column family'leri oluştur
    # İpucu: connection.create_table(table_name, families) ile tablo yarat
    # İpucu: Hata durumunda exception yakalayıp mesaj yazdır
    pass

def delete_tables(connection, config):
    """Var olan tabloları sil (temiz başlamak için)"""
    print(f"\n{'='*60}")
    print("🗑️  Var Olan Tablolar Siliniyor")
    print(f"{'='*60}")
    
    existing_tables = [t.decode('utf-8') for t in connection.tables()]
    
    for table_name in config.TABLES.keys():
        if table_name in existing_tables:
            try:
                connection.delete_table(table_name, disable=True)
                print(f"✅ Tablo silindi: {table_name}")
            except Exception as e:
                print(f"❌ Tablo silinirken hata ({table_name}): {e}")

# ====================================
# Spark Functions
# ====================================
def create_spark_session(config):
    """HDFS/MinIO okumak için Spark oturumu oluştur"""
    builder = SparkSession.builder \
        .appName("HBaseLoader") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g")
    
    # Configure for MinIO/S3
    if config.STORAGE_TYPE == "minio":
        builder = builder \
            .config("spark.jars.packages", 
                   "org.apache.hadoop:hadoop-aws:3.3.4,"
                   "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT) \
            .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                   "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    # Configure for HDFS
    elif config.STORAGE_TYPE == "hdfs":
        builder = builder \
            .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark

def load_and_insert_data(spark, connection, config):
    """Veriyi HDFS'den yükle ve HBase'e ekle"""
    print(f"\n{'='*60}")
    print("📥 HDFS'den Veri Yükleniyor ve HBase'e Ekleniyor")
    print(f"{'='*60}")
    
    total_records = 0
    
    # 1. Load Response Metrics
    if load_response_metrics(spark, connection, config):
        total_records += 1
    
    # 2. Load Service Errors
    if load_service_errors(spark, connection, config):
        total_records += 1
    
    # 3. Load Region Traffic
    if load_region_traffic(spark, connection, config):
        total_records += 1
    
    # 4. Load Hourly Traffic
    if load_hourly_traffic(spark, connection, config):
        total_records += 1
    
    # 5. Load Top Users
    if load_top_users(spark, connection, config):
        total_records += 1
    
    print(f"\n✅ {total_records} sonuç kümesi HBase'e yüklendi")

def load_response_metrics(spark, connection, config):
    """Yanıt süresi metriklerini HBase'e yükle"""
    table_name = "response_metrics"
    base_path = config.MINIO_RESULTS_PATH if config.STORAGE_TYPE == "minio" else config.HDFS_RESULTS_PATH
    path = f"{base_path}/response_time_metrics"
    
    print(f"\n📊 {table_name} yükleniyor...")
    
    # TODO 3: Response metrics verilerini Parquet'ten okuyup HBase'e yükle
    # İpucu: spark.read.parquet(path) ile veriyi oku
    # İpucu: connection.table(table_name) ile HBase tablosunu aç
    # İpucu: df.collect() ile satırları al, table.batch() ile batch işlemi başlat
    # İpucu: Her satır için row_key = f"endpoint_{idx:05d}".encode('utf-8') formatında key oluştur
    # İpucu: batch.put() ile veriyi ekle, kolonlar: metrics:endpoint, metrics:request_count, vs.
    # İpucu: batch.send() ile verileri gönder
    # İpucu: Başarı durumunda True, hata durumunda False döndür
    pass

def load_service_errors(spark, connection, config):
    """Servis hata istatistiklerini HBase'e yükle"""
    table_name = "service_errors"
    base_path = config.MINIO_RESULTS_PATH if config.STORAGE_TYPE == "minio" else config.HDFS_RESULTS_PATH
    path = f"{base_path}/service_errors"
    
    print(f"\n📊 {table_name} yükleniyor...")
    
    # TODO 4: Service errors verilerini Parquet'ten okuyup HBase'e yükle
    # İpucu: load_response_metrics() ile aynı mantık
    # İpucu: row_key olarak servis ismini kullan: str(row.service).encode('utf-8')
    # İpucu: Kolonlar: stats:service, stats:total_requests, stats:error_count, stats:warn_count, stats:error_rate, stats:warn_rate
    pass

def load_region_traffic(spark, connection, config):
    """Bölge trafik verisini HBase'e yükle"""
    table_name = "region_traffic"
    base_path = config.MINIO_RESULTS_PATH if config.STORAGE_TYPE == "minio" else config.HDFS_RESULTS_PATH
    path = f"{base_path}/region_traffic"
    
    print(f"\n📊 {table_name} yükleniyor...")
    
    # TODO 5: Region traffic verilerini Parquet'ten okuyup HBase'e yükle
    # İpucu: row_key olarak bölge ismini kullan: str(row.region).encode('utf-8')
    # İpucu: Kolonlar: traffic:region, traffic:request_count, traffic:avg_response_time
    pass

def load_hourly_traffic(spark, connection, config):
    """Saatlik trafik verisini HBase'e yükle"""
    table_name = "hourly_traffic"
    base_path = config.MINIO_RESULTS_PATH if config.STORAGE_TYPE == "minio" else config.HDFS_RESULTS_PATH
    path = f"{base_path}/hourly_traffic"
    
    print(f"\n📊 {table_name} yükleniyor...")
    
    # TODO 6: Hourly traffic verilerini Parquet'ten okuyup HBase'e yükle
    # İpucu: row_key formatı: f"hour_{row.hour:02d}".encode('utf-8')
    # İpucu: Kolonlar: traffic:hour, traffic:request_count, traffic:avg_response_time
    pass

def load_top_users(spark, connection, config):
    """En çok istek yapan kullanıcıları HBase'e yükle"""
    table_name = "top_users"
    base_path = config.MINIO_RESULTS_PATH if config.STORAGE_TYPE == "minio" else config.HDFS_RESULTS_PATH
    path = f"{base_path}/top_users"
    
    print(f"\n📊 {table_name} yükleniyor...")
    
    # TODO 7: Top users verilerini Parquet'ten okuyup HBase'e yükle
    # İpucu: row_key formatı: f"user_{row.user_id}".encode('utf-8')
    # İpucu: Kolonlar: user:user_id, user:request_count, user:rank
    # İpucu: enumerate() kullanarak rank değerini (idx + 1) olarak hesapla
    pass

def show_sample_data(connection, config):
    """HBase tablolarından örnek veri göster"""
    print(f"\n{'='*60}")
    print("📋 HBase Tablolarından Örnek Veri")
    print(f"{'='*60}")
    
    # TODO 8: HBase tablolarından örnek veri oku ve göster
    # İpucu: config.TABLES.keys() ile tablo isimlerini döngüyle gez
    # İpucu: connection.table(table_name) ile tabloyu aç
    # İpucu: table.scan(limit=3) ile ilk 3 satırı taramak için iterator al
    # İpucu: Her satırda key ve data var, data bir sözlük (kolon:değer)
    # İpucu: .decode('utf-8') ile byte'ları stringe çevir
    pass

# ====================================
# Main Execution
# ====================================
def main():
    """Ana çalıştırma fonksiyonu - HBase bağlantı testi"""
    print("\n" + "="*60)
    print("🚀 HBASE BAĞLANTI TESTİ BAŞLIYOR")
    print("="*60)
    
    start_time = time.time()
    config = Config()
    
    # HBase'e bağlan
    connection = connect_to_hbase(config)
    
    try:
        # TODO Fonksiyonları tamaladıktan sonra aşağıdaki yorumları kaldırın
        # Not: Temiz başlamak için var olan tabloları silmek isterseniz aşağıdaki satırı açın
        # delete_tables(connection, config)
    
        # Tabloları oluştur
        #create_tables(connection, config)
        
        # Spark oturumu oluştur
        #print(f"\nDepolama Tipi: {config.STORAGE_TYPE.upper()}")
        #spark = create_spark_session(config)
        
        # Veriyi yükle ve ekle
        #load_and_insert_data(spark, connection, config)
        
        # Örnek veri göster
        show_sample_data(connection, config)

    
        # Var olan tabloları listele
        print(f"\n{'='*60}")
        print("📋 Mevcut HBase Tabloları")
        print(f"{'='*60}")
        
        tables = [t.decode('utf-8') for t in connection.tables()]
        if tables:
            for idx, table_name in enumerate(tables, 1):
                print(f"   {idx}. {table_name}")
        else:
            print("   (Henüz tablo yok)")
        
        execution_time = time.time() - start_time
        
        print("\n" + "="*60)
        print("✅ YÜKLEME BAŞARILI")
        print("="*60)
        print(f"⏱️  Toplam Çalışma Süresi: {execution_time:.2f} saniye")
        print("🌐 HBase Web Arayüzü: http://localhost:16010")
        print("\n💡 İpucu: Diğer fonksiyonları TODO'lardan implement ederek veri yükleme yapabilirsiniz")
        
    except Exception as e:
        print(f"\n❌ Bağlantı testi sırasında hata: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        connection.close()
        print("\n🔌 HBase bağlantısı kapatıldı")

if __name__ == "__main__":
    main()
