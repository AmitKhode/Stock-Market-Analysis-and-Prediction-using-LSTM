import streamlit as st
import pandas as pd
import plotly.graph_objs as go
import os
import subprocess
import glob
import sys

# --- CONFIGURATION ---
PREDICTIONS_DIR = "/home/amitk/Desktop/stock-market-etl/Predictions"
CLEANED_DATA_DIR = "/home/amitk/Desktop/stock-market-etl/spark_output"
AVAILABLE_STOCKS = ["AAPL", "GOOG", "TSLA", "AMZN", "MSFT"]
PREDICTION_SCRIPT = "/home/amitk/Desktop/stock-market-etl/Predictions/predict.py"

st.set_page_config(page_title="📊 Stock Dashboard", layout="wide")
st.title("📈 Real-Time Stock Forecast Dashboard")

# --- SESSION STATE ---
if "selected_stock" not in st.session_state:
    st.session_state.selected_stock = "AAPL"

# --- SIDEBAR OPTIONS ---
st.sidebar.header("⚙️ Settings")

# Select Stock (session-preserving)
selected_stock = st.sidebar.selectbox(
    "Select Stock Symbol",
    AVAILABLE_STOCKS,
    index=AVAILABLE_STOCKS.index(st.session_state.selected_stock),
    key="selected_stock"
)

# Live Mode Toggle
live_mode = st.sidebar.checkbox("📡 Enable Live Auto-Refresh", value=False)
refresh_interval = st.sidebar.slider("⏱️ Auto-refresh every (sec)", 5, 60, 15)

# Manual Refresh Button
if st.sidebar.button("🔁 Refresh Now"):
    st.rerun()

# Optional Meta Refresh (Live Mode)
if live_mode:
    st.markdown(
        f"<meta http-equiv='refresh' content='{refresh_interval}'>",
        unsafe_allow_html=True
    )

# --- RUN PREDICTION BUTTON ---
if st.sidebar.button("🔮 Run LSTM Prediction"):
    with st.spinner("Predicting next hour price..."):
        try:
            subprocess.run([sys.executable, PREDICTION_SCRIPT, "--stock", selected_stock], check=True)
            st.success(f"Prediction completed for {selected_stock}")
        except subprocess.CalledProcessError:
            st.error("Prediction failed. Check logs or model status.")

# --- LOAD CLEANED DATA ---
def load_cleaned_data(stock):
    try:
        file = os.path.join(CLEANED_DATA_DIR, f"{stock}_cleaned.csv")
        df = pd.read_csv(file)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    except:
        return None

# --- LOAD PREDICTIONS ---
def load_predictions(stock):
    try:
        pattern = os.path.join(PREDICTIONS_DIR, f"{stock}_predictions.csv")
        files = glob.glob(pattern)
        if not files:
            return None
        df = pd.read_csv(files[0])
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    except:
        return None

# --- DISPLAY DATA ---
cleaned_df = load_cleaned_data(selected_stock)
pred_df = load_predictions(selected_stock)

st.subheader(f"📉 {selected_stock} Stock Price Forecast")

if cleaned_df is not None and pred_df is not None:
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=cleaned_df["timestamp"], y=cleaned_df["Close"], mode='lines', name='Actual'))
    fig.add_trace(go.Scatter(x=pred_df["timestamp"], y=pred_df["Predicted"], mode='markers+lines', name='Predicted'))
    fig.update_layout(title=f"{selected_stock} - Forecast vs Actual",
                      xaxis_title="Timestamp", yaxis_title="Price")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("Prediction or cleaned data not available yet.")

# --- LIVE DATA VIEW ---
st.subheader("📡 Live Data (Last 10 Rows)")
if cleaned_df is not None:
    st.dataframe(cleaned_df.tail(10).sort_values("timestamp"), use_container_width=True)
else:
    st.info("No cleaned data found yet.")

# --- FOOTER ---
st.markdown("---")

