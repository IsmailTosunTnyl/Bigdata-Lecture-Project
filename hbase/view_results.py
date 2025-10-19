"""
Streamlit ile HBase Sonuç Görüntüleyici.

Bu template Streamlit kullanarak oluşturuldu, başka görselleştirme tooları kullanmakta özgürsünüz.
Raporda ürettiğiniz görselleri kullanınız.
=======================================
HBase verilerini görüntülemek ve görselleştirmek için interaktif gösterge paneli

Çalıştırma Talimatları:
Dev-Ortamında bu komutu kullanın:
streamlit run hbase/view_results.py --server.address 0.0.0.0 --server.port 8501

"""

import streamlit as st
import happybase
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ====================================
# Configuration
# ====================================
HBASE_HOST = "hbase"
HBASE_PORT = 9090

# ====================================
# HBase Connection
# ====================================
@st.cache_resource
def get_hbase_connection():
    """HBase bağlantısı al (önbellekli)"""
    try:
        connection = happybase.Connection(
            host=HBASE_HOST,
            port=HBASE_PORT,
            timeout=30000
        )
        return connection
    except Exception as e:
        st.error(f"❌ HBase'e bağlanırken hata: {e}")
        return None

# ====================================
# Data Loading Functions
# ====================================
@st.cache_data(ttl=60)
def load_response_metrics(_connection):
    """HBase'den yanıt metriklerini yükle"""
    # TODO 1: response_metrics tablosundan veri oku ve DataFrame döndür
    # İpucu: _connection.table('response_metrics') ile tabloyu aç
    # İpucu: table.scan() ile tüm satırları tara
    # İpucu: Her satırda metrics:endpoint, metrics:request_count gibi kolonlar var
    # İpucu: .decode('utf-8') ile byte'ları stringe çevir, int() ve float() ile tiplere dönüştür
    # İpucu: pd.DataFrame(data) ile DataFrame oluştur
    pass

@st.cache_data(ttl=60)
def load_service_errors(_connection):
    """HBase'den servis hatalarını yükle"""
    # TODO 2: service_errors tablosundan veri oku ve DataFrame döndür
    # İpucu: stats:service, stats:total_requests, stats:error_count kolonlarını kullan
    pass

@st.cache_data(ttl=60)
def load_region_traffic(_connection):
    """HBase'den bölge trafiğini yükle"""
    # TODO 3: region_traffic tablosundan veri oku ve DataFrame döndür
    pass

@st.cache_data(ttl=60)
def load_hourly_traffic(_connection):
    """HBase'den saatlik trafiği yükle"""
    # TODO 4: hourly_traffic tablosundan veri oku, DataFrame'e çevir ve hour'a göre sırala
    pass

@st.cache_data(ttl=60)
def load_top_users(_connection):
    """HBase'den en çok istek yapan kullanıcıları yükle"""
    # TODO 5: top_users tablosundan veri oku, DataFrame'e çevir ve rank'e göre sırala
    pass

# ====================================
# Visualization Functions
# ====================================
def plot_response_times(df):
    """Yanıt süresi metriklerini görselleştir"""
    # TODO 6: Yanıt süresi verilerini görselleştir
    # İpucu: DataFrame'de endpoint, avg_response_time, median_response_time, p95, p99 kolonları var
    # İpucu: Streamlit için: st.plotly_chart(), st.pyplot(), st.bar_chart(), st.line_chart() kullanabilirsin
    # İpucu: Plotly için: px.bar(), px.line(), px.scatter(), go.Figure() kullanabilirsin
    # İpucu: Matplotlib için: plt.bar(), plt.plot(), plt.scatter() kullanabilirsin
    # İpucu: Seaborn için: sns.barplot(), sns.lineplot(), sns.heatmap() kullanabilirsin
    # Özgürce kendi görselleştirmeni tasarla!
    pass

def plot_service_errors(df):
    """Servis hata istatistiklerini görselleştir"""
    # TODO 7: Servis hata verilerini görselleştir
    # İpucu: DataFrame'de service, error_count, warn_count, error_rate, warn_rate kolonları var
    # İpucu: Çubuk grafik, pasta grafik, stacked bar chart gibi farklı yöntemler deneyebilirsin
    pass

def plot_region_traffic(df):
    """Bölge trafiğini görselleştir"""
    # TODO 8: Bölgesel trafik verilerini görselleştir
    # İpucu: DataFrame'de region, request_count, avg_response_time kolonları var
    # İpucu: Coğrafi dağılım için pasta, harita veya çubuk grafik kullanabilirsin
    pass

def plot_hourly_traffic(df):
    """Saatlik trafik desenini görselleştir"""
    # TODO 9: Saatlik trafik verilerini görselleştir
    # İpucu: DataFrame'de hour, request_count, avg_response_time kolonları var
    # İpucu: Zaman serisi grafiği, çizgi grafik veya alan grafiği kullanabilirsin
    pass

def plot_top_users(df):
    """En çok istek yapan kullanıcıları görselleştir"""
    # TODO 10: En aktif kullanıcıları görselleştir
    # İpucu: DataFrame'de user_id, request_count, rank kolonları var
    # İpucu: Yatay/dikey çubuk grafik veya lollipop chart kullanabilirsin
    pass

# ====================================
# Main App
# ====================================
def main():
    st.set_page_config(
        page_title="HBase Analitik Gösterge Paneli",
        page_icon="📊",
        layout="wide"
    )

    st.title("📊 Log Analitik Gösterge Paneli")
    st.markdown("Gerçek zamanlı analizler HBase üzerinden")

    # HBase'e bağlan
    connection = get_hbase_connection()

    if connection is None:
        st.error("HBase'e bağlanılamıyor. HBase'in çalıştığından emin olun.")
        st.code("docker ps | grep hbase")
        return

    # Yan menü
    st.sidebar.title("Navigasyon")
    page = st.sidebar.radio(
        "Görünüm Seçin",
        ["📈 Genel Bakış", "⚡ Yanıt Süreleri", "🚨 Hatalar", "🌍 Bölgesel Trafik", 
         "⏰ Saatlik Desenler", "👥 En Çok Kullanıcılar", "📋 Ham Veri"]
    )

    # Yenile butonu
    if st.sidebar.button("🔄 Veriyi Yenile"):
        st.cache_data.clear()
        st.rerun()

    # Genel Bakış Sayfası
    if page == "📈 Genel Bakış":
        st.header("Genel Bakış")

        # Tüm verileri yükle
        response_df = load_response_metrics(connection)
        errors_df = load_service_errors(connection)
        region_df = load_region_traffic(connection)
        hourly_df = load_hourly_traffic(connection)
        users_df = load_top_users(connection)

        # Özet metrikler
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            total_requests = region_df['request_count'].sum() if not region_df.empty else 0
            st.metric("Toplam İstek", f"{total_requests:,}")

        with col2:
            total_errors = errors_df['error_count'].sum() if not errors_df.empty else 0
            st.metric("Toplam Hata", f"{total_errors:,}")

        with col3:
            avg_error_rate = errors_df['error_rate'].mean() if not errors_df.empty else 0
            st.metric("Ortalama Hata Oranı", f"{avg_error_rate:.2f}%")

        with col4:
            regions = len(region_df) if not region_df.empty else 0
            st.metric("Bölge Sayısı", regions)

        st.markdown("---")

        # Hızlı görselleştirmeler
        col1, col2 = st.columns(2)

        with col1:
            if not region_df.empty:
                fig = px.pie(region_df, values='request_count', names='region', 
                           title='Bölgeye Göre Trafik')
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            if not errors_df.empty:
                fig = px.bar(errors_df, x='service', y='error_rate', 
                           title='Servis Bazında Hata Oranı')
                st.plotly_chart(fig, use_container_width=True)

    # Yanıt Süreleri Sayfası
    elif page == "⚡ Yanıt Süreleri":
        st.header("Yanıt Süresi Analizi")
        df = load_response_metrics(connection)

        if not df.empty:
            plot_response_times(df)

            st.subheader("Veri Tablosu")
            st.dataframe(df, use_container_width=True)
        else:
            st.warning("Veri yok. Önce HBase yükleyicisini çalıştırın.")

    # Hatalar Sayfası
    elif page == "🚨 Hatalar":
        st.header("Hata Analizi")
        df = load_service_errors(connection)

        if not df.empty:
            plot_service_errors(df)

            st.subheader("Veri Tablosu")
            st.dataframe(df, use_container_width=True)
        else:
            st.warning("Veri yok. Önce HBase yükleyicisini çalıştırın.")

    # Bölgesel Trafik Sayfası
    elif page == "🌍 Bölgesel Trafik":
        st.header("Bölgesel Trafik Analizi")
        df = load_region_traffic(connection)

        if not df.empty:
            plot_region_traffic(df)

            st.subheader("Veri Tablosu")
            st.dataframe(df, use_container_width=True)
        else:
            st.warning("Veri yok. Önce HBase yükleyicisini çalıştırın.")

    # Saatlik Desenler Sayfası
    elif page == "⏰ Saatlik Desenler":
        st.header("Saatlik Trafik Desenleri")
        df = load_hourly_traffic(connection)

        if not df.empty:
            plot_hourly_traffic(df)

            st.subheader("Veri Tablosu")
            st.dataframe(df, use_container_width=True)
        else:
            st.warning("Veri yok. Önce HBase yükleyicisini çalıştırın.")

    # En Çok Kullanıcılar Sayfası
    elif page == "👥 En Çok Kullanıcılar":
        st.header("En Çok İstek Yapan Kullanıcılar")
        df = load_top_users(connection)

        if not df.empty:
            plot_top_users(df)

            st.subheader("Veri Tablosu")
            st.dataframe(df, use_container_width=True)
        else:
            st.warning("Veri yok. Önce HBase yükleyicisini çalıştırın.")

    # Ham Veri Sayfası
    elif page == "📋 Ham Veri":
        st.header("Ham Veri İnceleyici")

        table_name = st.selectbox(
            "Tablo Seçin",
            ["response_metrics", "service_errors", "region_traffic", 
             "hourly_traffic", "top_users"]
        )

        if st.button("Veriyi Yükle"):
            try:
                table = connection.table(table_name)
                data = []

                for key, row_data in table.scan():
                    row = {'row_key': key.decode('utf-8')}
                    for col, val in row_data.items():
                        row[col.decode('utf-8')] = val.decode('utf-8')
                    data.append(row)

                if data:
                    df = pd.DataFrame(data)
                    st.dataframe(df, use_container_width=True)

                    # İndir butonu
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="CSV Olarak İndir",
                        data=csv,
                        file_name=f"{table_name}.csv",
                        mime="text/csv"
                    )
                else:
                    st.info("Bu tabloda veri yok")

            except Exception as e:
                st.error(f"Hata: {e}")

    # Alt bilgi
    st.sidebar.markdown("---")
    st.sidebar.info(
        """
        **HBase Gösterge Paneli**

        Veri Kaynağı: HBase @ hbase:9090

        🌐 [HBase Web Arayüzü](http://localhost:16010)
        """
    )

if __name__ == "__main__":
    main()
