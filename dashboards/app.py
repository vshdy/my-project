import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

st.set_page_config(page_title="Big Data Project: Анализ покупательского поведения", layout="wide")

st.title("Аналитика покупательского поведения")
st.markdown("Данные обработаны с помощью **Dask** и оркестрованы **Prefect**.")

@st.cache_resource
def get_connection():
    return create_engine("postgresql+pg8000://retail_user:retail_password@db:5432/retail_db")
           
try:
    engine = get_connection()
    df = pd.read_sql("SELECT * FROM customer_rfm", engine)

    col1, col2, col3 = st.columns(3)
    col1.metric("Всего уникальных клиентов", len(df))
    col2.metric("Средний чек (Monetary)", f"{df['monetary'].mean():.2f}")
    col3.metric("Средняя давность покупки (дней)", f"{df['recency'].mean():.1f}")

    st.divider()

    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Топ-10 клиентов по выручке")
        top_10 = df.nlargest(10, 'monetary').sort_values('monetary', ascending=True)
        
        fig_top = px.bar(
            top_10, 
            x='monetary', 
            y='CustomerID', 
            orientation='h',  
            color='monetary',
            color_continuous_scale='Viridis', 
            labels={'CustomerID': 'ID Клиента', 'monetary': 'Выручка'},
            text_auto='.2s'
        )
        

        fig_top.update_layout(yaxis={'type': 'category'}) 
 
        fig_top.update_layout(margin=dict(l=0, r=0, t=30, b=0))
        
        st.plotly_chart(fig_top, use_container_width=True)

    with c2:
        st.subheader("Связь: Частота vs Выручка (RFM-анализ)\n(анализ лояльности: как часто клиенты возвращаются и сколько приносят дохода)")

        fig_scatter = px.scatter(
            df, 
            x="frequency", 
            y="monetary",
            color="recency",
            size="avg_unit_price",
            hover_data=['CustomerID'],
            log_x=True,
            log_y=True,
            color_continuous_scale='Viridis',
            size_max=40, 
            labels={
                'frequency': 'Кол-во заказов (log)',
                'monetary': 'Сумма выручки (log)',
                'recency': 'Дней с покупки'
            },
            template="plotly_dark"
        )

        fig_scatter.update_traces(
            marker=dict(
                opacity=0.9,        
                line=dict(
                    width=0.5,      
                    color='rgba(255, 255, 255, 0.2)'
                )
            ),
            selector=dict(mode='markers')
        )


        fig_scatter.update_layout(margin=dict(l=0, r=0, t=30, b=0))

        st.plotly_chart(fig_scatter, use_container_width=True)

    st.divider()

    c3, c4 = st.columns(2)

    with c3:
        st.subheader("Тепловая карта активности")
        st.caption("(Когда клиенты чаще всего делают заказы)")

        try:
            df_raw = pd.read_csv('data/online_retail.csv', encoding='ISO-8859-1')
            df_raw['InvoiceDate'] = pd.to_datetime(df_raw['InvoiceDate'])
            
            df_raw['hour'] = df_raw['InvoiceDate'].dt.hour
            df_raw['day_of_week'] = df_raw['InvoiceDate'].dt.day_name()

            df_time_stats = df_raw.groupby(['day_of_week', 'hour']).size().reset_index(name='order_count')

            days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            df_time_stats['day_of_week'] = pd.Categorical(df_time_stats['day_of_week'], categories=days_order, ordered=True)

            full_index = pd.MultiIndex.from_product([days_order, range(24)], names=['day_of_week', 'hour'])
            df_time_stats = df_time_stats.set_index(['day_of_week', 'hour']).reindex(full_index, fill_value=0).reset_index()

   
            df_heatmap = df_time_stats.pivot(index='day_of_week', columns='hour', values='order_count')

            fig_heat = px.imshow(
                df_heatmap,
                labels=dict(x="Час суток", y="День недели", color="Заказов"),
                x=list(range(24)), 
                y=days_order,
                color_continuous_scale='Viridis',
                aspect="auto"
            )

            fig_heat.update_layout(
                xaxis_nticks=24,
                xaxis=dict(tickmode='linear', tick0=0, dtick=1)
            )
            
            st.plotly_chart(fig_heat, use_container_width=True)

        except Exception as e:
            st.error(f"Ошибка при построении тепловой карты: {e}")



    with c4:
        st.subheader("Общая статистика по сегментам")
        stats = df[['recency', 'frequency', 'monetary', 'avg_unit_price']].describe().T
        st.dataframe(stats[['mean', '50%', 'max']], use_container_width=True)

    
    st.divider()
    c5, c6 = st.columns(2)

    with c5:
        st.subheader("Распределение давности \n(через сколько дней клиенты возвращаются: анализ активности и выявление уходящих покупателей)")
        fig_rec = px.histogram(df, x="recency", nbins=30, 
                               labels={'recency': 'Дней с последней покупки'},
                               color_discrete_sequence=['#00CC96'])
        st.plotly_chart(fig_rec, use_container_width=True)

    with c6:
        st.subheader("Топ-10 стран по числу клиентов")
        country_counts = df['Country'].value_counts().nlargest(10).reset_index()
        country_counts.columns = ['Country', 'ClientCount']
        
        fig_country = px.pie(country_counts, values='ClientCount', names='Country',
                             hole=0.4,
                             color_discrete_sequence=px.colors.sequential.RdBu)
        st.plotly_chart(fig_country, use_container_width=True)

except Exception as e:
    st.error("Не удалось подключиться к базе данных. Убедись, что Docker запущен и ETL-флоу отработал.")
    st.info(f"Ошибка: {e}")