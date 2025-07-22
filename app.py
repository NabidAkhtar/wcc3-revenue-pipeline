import streamlit as st
import pandas as pd
import os
import plotly.express as px
import io, base64, shutil, requests, hmac
from datetime import datetime
from pathlib import Path
from pipeline_core import RevenuePipeline, PipelineConfig

# ---------- Password Protection ----------
def check_password():
    def password_entered():
        if hmac.compare_digest(st.session_state["password"], st.secrets["password"]):
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False
    if "password_correct" not in st.session_state:
        st.text_input("Password", type="password", on_change=password_entered, key="password")
        return False
    if not st.session_state["password_correct"]:
        st.text_input("Password", type="password", on_change=password_entered, key="password")
        st.error("😕 Password incorrect")
        return False
    return True

if not check_password():
    st.stop()

# ---------- Google Cloud Credentials from st.secrets ----------
from google.oauth2 import service_account
gcp_creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])

st.set_page_config(
    page_title="Nazara Revenue Dashboard",
    page_icon="🏏",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .block-container { padding-top: 0rem !important; }
    h1 { font-size: 2.2rem; font-weight: 600; color: #254068; text-align: center; margin: 1rem 0 2rem 0; letter-spacing: 1px; }
    .upload-section, .pack-section {padding:1rem;border-radius:8px;background:#f6f8fb;margin-bottom:1rem;}
    .metric-card, .success-card, .warning-card, .error-card {
        padding: 0.9rem; border-radius: 8px; color: #25334a; margin: 0.5rem 0; font-weight: 500; letter-spacing: 0.2px;
        background: #eef1f6;
        border: 1px solid #dde1ea;
    }
    .metric-card { background: #ebf1f7; color: #2b4267; border:1px solid #cedaec; }
    .success-card { background: #e1f3ea; color: #206647; border:1px solid #b7e2c7;}
    .warning-card{background:#fff7e7; color: #7f6c34; border:1px solid #ebdeb0;}
    .error-card {background:#fbeaea; color: #973137; border:1px solid #ecc7c9;}
    .stButton > button { background: #e9eff7; color: #254068; border-radius: 5px; font-weight: 500; border:1px solid #d1d9e0;}
    .stButton > button:hover {background: #c7dbf1;}
    .sidebar .sidebar-content {background: #f8fafb;}
    .logo-img { display: flex; justify-content: center; }
    .logo-img img { max-width: 250px; height: auto; display: block; margin-left:auto; margin-right:auto; padding-top:26px; }
</style>
""", unsafe_allow_html=True)

def get_image_html(image_path: str, class_name: str) -> str:
    if not os.path.exists(image_path): return ""
    with open(image_path, "rb") as f:
        img_bytes = f.read()
    b64_string = base64.b64encode(img_bytes).decode()
    return (
        f'<div class="{class_name}">'
        f'<img src="data:image/png;base64,{b64_string}" alt="logo">'
        f'</div>'
    )

for key in ['pipeline_results', 'processing_logs', 'config', 'pipeline_status']:
    if key not in st.session_state:
        st.session_state[key] = None if key != 'processing_logs' else []

# Branding/Header
st.markdown(get_image_html("nazara.png", "logo-img"), unsafe_allow_html=True)
st.markdown('<h1>Nazara Revenue Pipeline</h1>', unsafe_allow_html=True)

def main():
    with st.sidebar:
        with st.container():
            st.markdown('<div class="upload-section"><b>1. Upload Cohort ZIP</b>', unsafe_allow_html=True)
            uploaded_zip = st.file_uploader(
                "", type=["zip"], accept_multiple_files=False,
                help="Upload a zip file containing the required folder structure."
            )
            st.markdown('</div>', unsafe_allow_html=True)

        with st.container():
            st.markdown('<div class="pack-section"><b>2. Select Pack Types</b>', unsafe_allow_html=True)
            default_packs = [
                "premium_packs_with_ad_ids.csv",
                "career_packs_with_ad_ids.csv",
                "event_packs_with_ad_ids.csv",
                "micro_packs_with_ad_ids.csv",
                "npl_packs_with_ad_ids.csv",
                "stage1_top_25k_with_ad_ids.csv"
            ]
            pack_types = st.multiselect(
                "Pack Types", options=default_packs, default=default_packs,
                help="Choose which pack types to include in the pipeline"
            )
            st.markdown('</div>', unsafe_allow_html=True)

        if st.button("💾 Save Configuration", use_container_width=True):
            if uploaded_zip:
                temp_dir = "temp_uploads"
                if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
                os.makedirs(temp_dir)
                import zipfile
                with zipfile.ZipFile(uploaded_zip, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                extracted_items = os.listdir(temp_dir)
                main_folder_path = os.path.join(temp_dir, extracted_items[0]) if len(extracted_items) == 1 and os.path.isdir(os.path.join(temp_dir, extracted_items[0])) else temp_dir
                output_folder = "output_results"
                config = PipelineConfig(
                    main_folder=main_folder_path,
                    output_folder=output_folder,
                    service_account_path=None,   # Not used (but left for compatibility)
                    window_size=1,
                    batch_size=100_000,
                    chunk_size=2000,
                    fallback_rate=86.191,
                    use_live_rates=True,
                    pack_types=pack_types,
                    gcp_creds=gcp_creds
                )
                st.session_state.config = config
                st.success("✅ Configuration saved! You may now execute the pipeline.")
            else:
                st.warning("Please upload a ZIP file with cohort data.")

        st.divider()
        st.header("⚡ Quick Actions")
        if st.button("🔄 Refresh Data", use_container_width=True): st.rerun()
        if st.button("💾 Download Results", use_container_width=True): download_results()
        if st.button("📋 View Logs", use_container_width=True): show_logs()

    col1, col2 = st.columns([2, 1])
    with col1:
        st.header("🚀 Pipeline")
        if st.session_state.config is not None:
            execution_tab, monitoring_tab, results_tab = st.tabs(["Execute", "Monitor", "Results"])
            with execution_tab: show_execution_interface()
            with monitoring_tab: show_monitoring_interface()
            with results_tab: show_results_interface()
        else:
            st.warning("Configure and save pipeline in the sidebar first.")
    with col2:
        st.header("📈 System & Activity")
        show_status_metrics()
        st.subheader("🕑 Recent Logs")
        show_recent_activity()

def show_execution_interface():
    st.subheader("Start Your Pipeline")
    execution_mode = st.radio(
        "Mode", options=["Full Pipeline", "Specific Cohorts"], horizontal=True,
        help="Process all cohorts or select specific ones"
    )
    selected_cohorts = []
    if execution_mode == "Specific Cohorts":
        available_cohorts = get_available_cohorts()
        selected_cohorts = st.multiselect("Cohorts", options=available_cohorts, help="Choose specific cohorts to process")
        st.session_state['selected_cohorts'] = selected_cohorts
    col1, col2 = st.columns([2,1])
    with col1:
        if st.button("▶️ Start Pipeline", use_container_width=True, type="primary"):
            st.session_state.pipeline_status = {'status': 'running', 'total_cohorts': 0, 'processed_cohorts': 0, 'processing_time': 0, 'total_revenue': 0}
            execute_pipeline(execution_mode)
    with col2:
        if st.button("⏹️ Stop", use_container_width=True): stop_pipeline()

def show_monitoring_interface():
    st.subheader("Live Monitoring")
    status = st.session_state.pipeline_status if isinstance(st.session_state.pipeline_status, dict) else None
    if status:
        fraction = status.get('processed_cohorts', 0)/max(1,status.get('total_cohorts', 1))
        st.progress(fraction)
        c1, c2, c3, c4 = st.columns(4)
        with c1: st.metric("Total Cohorts", status.get('total_cohorts', 0))
        with c2: st.metric("Completed", status.get('processed_cohorts', 0))
        with c3: st.metric("Time Elapsed", f"{status.get('processing_time', 0):.1f}s")
        with c4: st.metric("Total Revenue", f"₹{status.get('total_revenue', 0):,.0f}")
        st.subheader("Pipeline Log")
        logs_display = st.session_state.processing_logs[-10:] if st.session_state.processing_logs else []
        for log in logs_display:
            lt, msg, lvl = log['timestamp'],log['message'],log['level']
            if lvl == "ERROR": st.error(f"[{lt}] {msg}")
            elif lvl == "WARNING": st.warning(f"[{lt}] {msg}")
            else: st.info(f"[{lt}] {msg}")
    else:
        st.info("Pipeline not yet started.")

def show_results_interface():
    st.subheader("Results & Insights")
    results = st.session_state.pipeline_results
    if results:
        c1, c2, c3, c4 = st.columns(4)
        total_revenue = sum(r['Total Revenue'] for r in results)
        with c1: st.metric("Total Revenue", f"₹{total_revenue:,.0f}")
        with c2: st.metric("Cohorts", len(results))
        with c3: st.metric("Avg/Cohort", f"₹{total_revenue/len(results):,.0f}" if results else 0)
        with c4:
            best_cohort = max(results, key=lambda x: x['Total Revenue'])
            st.metric("Best Cohort", f"₹{best_cohort['Total Revenue']:,.0f}")
        st.dataframe(pd.DataFrame(results), use_container_width=True)
        st.subheader("Analytics")
        df = pd.DataFrame(results)
        st.plotly_chart(px.bar(df, x="Cohort", y="Total Revenue", color="Total Revenue", color_continuous_scale="Blues", title="Revenue per Cohort"), use_container_width=True)
        pack_cols = [col for col in df.columns if 'Revenue' in col and col != 'Total Revenue']
        if pack_cols:
            st.plotly_chart(
                px.pie(values=[df[col].sum() for col in pack_cols], names=[col.replace(' Revenue','') for col in pack_cols],
                       title="Pack Revenue Distribution", color_discrete_sequence=px.colors.qualitative.Pastel),
                use_container_width=True
            )
        if len(df) > 1 and 'Cohort' in df:
            try:
                df2 = df.copy()
                df2['Cohort_Date'] = pd.to_datetime(df2['Cohort'].str.extract(r'(\d+)')[0], format='%d')
                st.plotly_chart(
                    px.line(df2.sort_values('Cohort_Date'), x='Cohort', y='Total Revenue', markers=True, title='Revenue Trend',
                            line_shape="linear", color_discrete_sequence=["#5277a6"]),
                    use_container_width=True)
            except: pass
    else:
        st.info("No results. Run the pipeline to see results.")

def get_available_cohorts():
    if st.session_state.config and os.path.exists(st.session_state.config.main_folder):
        folders = [f.name for f in Path(st.session_state.config.main_folder).iterdir() if f.is_dir()]
        return sorted(folders, key=lambda x: int(x.split('_')[0]) if x.split('_')[0].isdigit() else 0)
    return []

def execute_pipeline(execution_mode):
    if st.session_state.config is None:
        st.error("❌ No configuration found!")
        return
    pipeline = RevenuePipeline(st.session_state.config)
    st.session_state.pipeline_status = {'status': 'running', 'total_cohorts': 0, 'processed_cohorts': 0, 'processing_time': 0, 'total_revenue': 0}
    log("INFO", f"Pipeline started: {execution_mode}")
    try:
        if execution_mode == "Full Pipeline":
            results = pipeline.run_full_pipeline()
        elif execution_mode == "Specific Cohorts":
            results = pipeline.run_specific_cohorts(st.session_state.get('selected_cohorts', []))
        st.session_state.pipeline_results = results
        st.session_state.pipeline_status['status'] = 'completed'
        log("INFO", "Pipeline completed.")
        st.success("Pipeline executed successfully!")
    except Exception as e:
        st.session_state.pipeline_status['status'] = 'error'
        log("ERROR", f"Pipeline failed: {str(e)}")
        st.error(f"Pipeline failed: {str(e)}")

def stop_pipeline():
    st.session_state.pipeline_status = {'status': 'idle'}
    log("WARNING", "Pipeline stopped.")
    st.warning("Stop triggered.")

def log(level, message):
    entry = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'level': level,
        'message': message
    }
    st.session_state.processing_logs.append(entry)
    if len(st.session_state.processing_logs) > 100:
        st.session_state.processing_logs = st.session_state.processing_logs[-100:]

def show_logs():
    st.subheader("All Logs")
    if st.session_state.processing_logs:
        st.dataframe(pd.DataFrame(st.session_state.processing_logs), use_container_width=True)
    else:
        st.info("No logs.")

def download_results():
    if st.session_state.pipeline_results is not None:
        output = io.BytesIO()
        df = pd.DataFrame(st.session_state.pipeline_results)
        df.to_excel(output, index=False)
        output.seek(0)
        st.download_button(
            label="📥 Download Results Excel",
            data=output.getvalue(),
            file_name=f"revenue_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.warning("No results to download.")

def show_status_metrics():
    status = st.session_state.pipeline_status if isinstance(st.session_state.pipeline_status, dict) else {}
    s_state = status.get('status', 'idle')
    col_msg = "metric-card"
    if s_state == 'completed': col_msg = "success-card"
    elif s_state == 'error': col_msg = "error-card"
    elif s_state == 'idle': col_msg = "warning-card"
    st.markdown(f'<div class="{col_msg}">{s_state.capitalize()}</div>', unsafe_allow_html=True)
    st.subheader("System")
    if st.session_state.config:
        c = st.session_state.config
        st.write(f"📁 Data: {'✅' if os.path.exists(c.main_folder) else '❌'}")
        st.write(f"🔑 GCP Credentials: {'✅' if getattr(c, 'gcp_creds', None) else '❌'}")
    try:
        r = requests.get("https://api.frankfurter.app/latest?from=USD&to=INR",timeout=5)
        if r.status_code == 200: rate = r.json()['rates']['INR']; st.write(f"💱 USD/INR: {rate:.2f}")
        else: st.write("💱 API Error")
    except: st.write("💱 API Failure")

def show_recent_activity():
    if st.session_state.processing_logs:
        for log in reversed(st.session_state.processing_logs[-5:]):
            ts, msg, lvl = log['timestamp'], log['message'], log['level']
            if lvl == "ERROR": st.error(f"🔴 {ts}: {msg}")
            elif lvl == "WARNING": st.warning(f"🟡 {ts}: {msg}")
            else: st.info(f"🔵 {ts}: {msg}")
    else:
        st.info("No recent logs.")

if __name__ == "__main__":
    main()
