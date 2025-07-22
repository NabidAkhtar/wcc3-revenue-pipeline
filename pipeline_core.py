import pandas as pd
import os
from datetime import datetime, timedelta
from google.cloud import bigquery
import requests
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import streamlit as st
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache

@dataclass
class PipelineConfig:
    main_folder: str
    output_folder: str
    service_account_path: str = None # for compatibility
    window_size: int = 3
    batch_size: int = 50000
    chunk_size: int = 1000
    fallback_rate: float = 86.191
    use_live_rates: bool = True
    pack_types: List[str] = None
    gcp_creds: Any = None

    def __post_init__(self):
        if self.pack_types is None:
            self.pack_types = [
                "premium_packs_with_ad_ids.csv",
                "career_packs_with_ad_ids.csv",
                "event_packs_with_ad_ids.csv",
                "micro_packs_with_ad_ids.csv",
                "npl_packs_with_ad_ids.csv",
                "stage1_top_25k_with_ad_ids.csv"
            ]

class RevenuePipeline:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.client = None
        self.user_cache = {}
        self.current_progress = 0
        self.total_cohorts = 0
        self.processed_cohorts = 0
        self.start_time = None

    def initialize_client(self):
        try:
            self.client = bigquery.Client(credentials=self.config.gcp_creds)
            self.add_log("INFO", "BigQuery client initialized successfully")
            return True
        except Exception as e:
            self.add_log("ERROR", f"Failed to initialize BigQuery client: {str(e)}")
            return False

    def add_log(self, level: str, message: str):
        if 'processing_logs' not in st.session_state:
            st.session_state.processing_logs = []
        log_entry = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'level': level,
            'message': message
        }
        st.session_state.processing_logs.append(log_entry)
        if len(st.session_state.processing_logs) > 100:
            st.session_state.processing_logs = st.session_state.processing_logs[-100:]

    def update_progress(self, processed: int, total: int, revenue: float = 0):
        if 'pipeline_status' not in st.session_state:
            st.session_state.pipeline_status = {}
        st.session_state.pipeline_status.update({
            'processed_cohorts': processed,
            'total_cohorts': total,
            'processing_time': time.time() - self.start_time if self.start_time else 0,
            'total_revenue': revenue
        })

    def get_cohort_folders(self) -> List[str]:
        try:
            if not os.path.exists(self.config.main_folder):
                self.add_log("ERROR", f"Main folder not found: {self.config.main_folder}")
                return []
            folders = [f.name for f in Path(self.config.main_folder).iterdir() if f.is_dir()]
            folders.sort(key=lambda x: int(x.split('_')[0]) if x.split('_')[0].isdigit() else 0)
            self.add_log("INFO", f"Found {len(folders)} cohort folders")
            return folders
        except Exception as e:
            self.add_log("ERROR", f"Error getting cohort folders: {str(e)}")
            return []

    def extract_unique_user_ids(self, csv_paths: List[str]) -> List[str]:
        key = tuple(sorted(csv_paths))
        if key in self.user_cache:
            return self.user_cache[key]
        user_ids = set()
        processed_files = 0
        for csv_path in csv_paths:
            if not os.path.exists(csv_path):
                self.add_log("WARNING", f"CSV file not found: {csv_path}")
                continue
            try:
                df = pd.read_csv(csv_path, usecols=['user_pseudo_id'])
                valid_ids = df['user_pseudo_id'].dropna().astype(str).str.strip()
                valid_ids = valid_ids[valid_ids != '']
                user_ids.update(valid_ids)
                processed_files += 1
                self.add_log("INFO", f"Processed {len(df)} rows from {os.path.basename(csv_path)}")
            except Exception as e:
                self.add_log("ERROR", f"Error processing {csv_path}: {str(e)}")
        user_ids = [uid for uid in user_ids if uid and str(uid).strip()]
        self.user_cache[key] = user_ids
        self.add_log("INFO", f"Extracted {len(user_ids)} unique user IDs from {processed_files} files")
        return user_ids

    def get_date_range(self, cohort_name: str) -> Tuple[Optional[str], Optional[str]]:
        try:
            # Parse cohort name like "1_july", "15_july"
            parts = cohort_name.split('_')
            if len(parts) != 2:
                raise ValueError("Cohort name must be in format 'day_month'")
            day = int(parts[0])
            month_name = parts[1].lower()
            # Map month names to numbers
            month_map = {
                'january': 1, 'jan': 1,
                'february': 2, 'feb': 2,
                'march': 3, 'mar': 3,
                'april': 4, 'apr': 4,
                'may': 5,
                'june': 6, 'jun': 6,
                'july': 7, 'jul': 7,
                'august': 8, 'aug': 8,
                'september': 9, 'sep': 9,
                'october': 10, 'oct': 10,
                'november': 11, 'nov': 11,
                'december': 12, 'dec': 12
            }
            if month_name not in month_map:
                raise ValueError(f"Invalid month name: {month_name}")
            month = month_map[month_name]
            # Use current year (2025)
            current_year = datetime.now().year
            start_date = datetime(current_year, month, day)
            end_date = start_date + timedelta(days=self.config.window_size - 1)
            self.add_log("INFO", f"Date range for {cohort_name}: {start_date.date()} to {end_date.date()}")
            return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
        except Exception as e:
            self.add_log("ERROR", f"Error parsing cohort name {cohort_name}: {str(e)}")
            return None, None

    @lru_cache(maxsize=32)
    def get_exchange_rate(self, start_date: str, end_date: str) -> float:
        if not self.config.use_live_rates:
            self.add_log("INFO", f"Using fallback exchange rate: {self.config.fallback_rate}")
            return self.config.fallback_rate
        try:
            url = f"https://api.frankfurter.app/{start_date}..{end_date}"
            params = {"from": "USD", "to": "INR"}
            response = requests.get(url, params=params, timeout=10)
            if response.status_code != 200:
                self.add_log("WARNING", f"Exchange rate API error: {response.text}")
                return self.config.fallback_rate
            data = response.json()
            rates = data.get("rates", {})
            inr_values = [rate["INR"] for rate in rates.values() if "INR" in rate]
            if not inr_values:
                self.add_log("WARNING", "No INR exchange rate data found")
                return self.config.fallback_rate
            avg_rate = sum(inr_values) / len(inr_values)
            self.add_log("INFO", f"Average USD to INR rate: {avg_rate:.4f}")
            return avg_rate
        except Exception as e:
            self.add_log("ERROR", f"Error fetching exchange rate: {str(e)}")
            return self.config.fallback_rate

    def run_query(self, user_ids: List[str], start_date: str, end_date: str, avg_rate: float) -> pd.DataFrame:
        if not user_ids:
            self.add_log("WARNING", "Empty user IDs list")
            return pd.DataFrame()
        all_results = []
        for i in range(0, len(user_ids), self.config.chunk_size):
            chunk = user_ids[i:i + self.config.chunk_size]
            chunk_list = ', '.join(['"{}"'.format(uid) for uid in chunk])
            query = f"""
            SELECT
                event_date,
                product_id,
                product_value,
                ROUND(product_value * {avg_rate}, 0) as product_value_INR,
                user_pseudo_id
            FROM `wcc3-live.dataset_zero.Product_Table_Streaming`
            WHERE event_date BETWEEN "{start_date}" AND "{end_date}"
            AND user_pseudo_id IN UNNEST([{chunk_list}])
            """
            try:
                self.add_log("INFO", f"Running query chunk {i//self.config.chunk_size + 1} with {len(chunk)} users")
                query_job = self.client.query(query)
                chunk_df = query_job.to_dataframe()
                if not chunk_df.empty:
                    all_results.append(chunk_df)
            except Exception as e:
                self.add_log("ERROR", f"Error in query chunk {i//self.config.chunk_size + 1}: {str(e)}")
                continue
        if all_results:
            df = pd.concat(all_results, ignore_index=True)
            self.add_log("INFO", f"Query completed with {len(df)} total records")
            return df
        return pd.DataFrame()

    def get_pack_csv_paths(self, cohort_group):
        pack_paths = []
        for csv_file in self.config.pack_types:
            pack_name = csv_file.replace("_packs_with_ad_ids.csv", "").replace("stage1_top_25k_with_ad_ids.csv", "stage1_top_25k")
            csv_paths = [str(Path(self.config.main_folder) / cohort / csv_file) for cohort in cohort_group]
            pack_paths.append((pack_name, csv_paths, csv_file))
        return pack_paths

    def process_pack(self, pack_name, csv_paths, start_date, end_date, avg_rate, output_dir):
        self.add_log("INFO", f"Processing {pack_name} pack")
        user_ids = self.extract_unique_user_ids(csv_paths)
        if not user_ids:
            self.add_log("WARNING", f"No IDs for {pack_name}")
            return 0
        total_revenue = 0
        all_batches = []
        for i in range(0, len(user_ids), self.config.batch_size):
            batch_users = user_ids[i:i + self.config.batch_size]
            batch_num = (i // self.config.batch_size) + 1
            batch_df = self.run_query(batch_users, start_date, end_date, avg_rate)
            if batch_df is not None and not batch_df.empty:
                all_batches.append(batch_df)
        if all_batches:
            full_df = pd.concat(all_batches)
            total_revenue = full_df['product_value_INR'].sum()
            full_df.to_csv(Path(output_dir) / f"{pack_name}.csv", index=False)
        self.add_log("INFO", f"{pack_name} revenue: ₹{total_revenue:,.0f}")
        return total_revenue

    def process_cohort_group(self, cohort_group: List[str]) -> Optional[Dict[str, Any]]:
        cohort_label = cohort_group[0]
        self.add_log("INFO", f"Processing cohort group: {cohort_label}")
        start_date, end_date = self.get_date_range(cohort_label)
        if not start_date or not end_date:
            self.add_log("ERROR", f"Invalid dates for {cohort_label}")
            return None
        avg_rate = self.get_exchange_rate(start_date, end_date)
        cohort_results = {"Cohort": cohort_label, "Total Revenue": 0}
        output_dir = Path(self.config.output_folder) / cohort_label
        os.makedirs(output_dir, exist_ok=True)
        pack_results = {}
        packs = self.get_pack_csv_paths(cohort_group)
        with ThreadPoolExecutor(max_workers=6) as executor:
            future_to_pack = {
                executor.submit(self.process_pack, pack_name, csv_paths, start_date, end_date, avg_rate, output_dir): pack_name
                for (pack_name, csv_paths, _) in packs
            }
            for future in as_completed(future_to_pack):
                pack_name = future_to_pack[future]
                try:
                    revenue = future.result()
                    pack_results[f"{pack_name.capitalize()} Revenue"] = revenue
                    cohort_results["Total Revenue"] += revenue
                except Exception as exc:
                    self.add_log("ERROR", f"{pack_name} generated an exception: {exc}")
                    pack_results[f"{pack_name.capitalize()} Revenue"] = 0
        cohort_results.update(pack_results)
        self.add_log("INFO", f"Completed cohort {cohort_label}: ₹{cohort_results['Total Revenue']:,.0f}")
        return cohort_results

    def run_full_pipeline(self) -> List[Dict[str, Any]]:
        self.start_time = time.time()
        if not self.initialize_client():
            raise Exception("Failed to initialize BigQuery client")
        os.makedirs(self.config.output_folder, exist_ok=True)
        cohort_folders = self.get_cohort_folders()
        if not cohort_folders:
            raise Exception("No cohort folders found")
        self.total_cohorts = len(cohort_folders) // self.config.window_size
        self.update_progress(0, self.total_cohorts)
        results = []
        total_revenue = 0
        for i in range(0, len(cohort_folders), self.config.window_size):
            cohort_group = cohort_folders[i:i + self.config.window_size]
            if len(cohort_group) == self.config.window_size:
                result = self.process_cohort_group(cohort_group)
                if result:
                    results.append(result)
                    total_revenue += result['Total Revenue']
                    self.processed_cohorts += 1
                    self.update_progress(self.processed_cohorts, self.total_cohorts, total_revenue)
        if results:
            self.save_summary(results)
        self.add_log("INFO", f"Pipeline completed with {len(results)} cohort groups processed")
        return results

    def run_specific_cohorts(self, selected_cohorts: List[str]) -> List[Dict[str, Any]]:
        self.start_time = time.time()
        if not self.initialize_client():
            raise Exception("Failed to initialize BigQuery client")
        if not selected_cohorts:
            raise Exception("No cohorts selected")
        cohort_groups = []
        for i in range(0, len(selected_cohorts), self.config.window_size):
            cohort_group = selected_cohorts[i:i + self.config.window_size]
            cohort_groups.append(cohort_group)
        self.total_cohorts = len(cohort_groups)
        self.update_progress(0, self.total_cohorts)
        results = []
        total_revenue = 0
        for cohort_group in cohort_groups:
            result = self.process_cohort_group(cohort_group)
            if result:
                results.append(result)
                total_revenue += result['Total Revenue']
                self.processed_cohorts += 1
                self.update_progress(self.processed_cohorts, self.total_cohorts, total_revenue)
        self.add_log("INFO", f"Specific cohorts pipeline completed")
        return results

    def save_summary(self, results: List[Dict[str, Any]]):
        try:
            df = pd.DataFrame(results)
            columns = ["Cohort", "Total Revenue"]
            pack_columns = [col for col in df.columns if 'Revenue' in col and col != 'Total Revenue']
            columns.extend(pack_columns)
            df = df[columns]
            output_excel = Path(self.config.output_folder) / "revenue_summary.xlsx"
            df.to_excel(output_excel, index=False)
            self.add_log("INFO", f"Summary saved at {output_excel}")
        except Exception as e:
            self.add_log("ERROR", f"Error saving summary: {str(e)}")
