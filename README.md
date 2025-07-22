**An automated, secure, and interactive dashboard for cohort-level revenue analytics for the WCC3 game.**

## 🎯 What is This?

The **Nazara Revenue Pipeline Dashboard** is a production-ready Streamlit application designed to:

- Automate revenue calculation for multiple user cohorts
- Connect directly with BigQuery to fetch transaction records
- Support pack-level and cohort-level revenue breakdown
- Provide clear visual analytics and Excel export-ready outputs
- Work entirely in the browser, with secure GCP and password integration

This tool transforms daily or weekly cohort analysis into a clean and repeatable process with live monitoring and visual revenue reporting.

##🎨 User Experience
┌─ Sidebar ─────────────────┐  ┌─ Main Dashboard ────────────────────────┐
│ 📁 Upload Cohort ZIP      │  │ 🚀 Pipeline Execution                  │
│ 📦 Select Pack Types      │  │   ├── Execute (Start/Stop controls)    │
│ 💾 Save Configuration     │  │   ├── Monitor (Live progress & logs)   │
│                            │  │   └── Results (Analytics & download)    │
│ ⚡ Quick Actions          │  │                                        │
│   🔄 Refresh Data         │  │ 📊 Revenue Visualizations              │
│   📥 Download Results     │  │   ├── Bar charts by cohort             │
│   📋 View All Logs        │  │   ├── Pie charts by pack type          │
└───────────────────────────┘  │   └── Trend lines over time            │
                               └─────────────────────────────────────────┘

## ⚙️ Features

### 📂 Upload & Configure  
- Upload a compressed `.zip` containing all cohort folders  
- Automatically validates and sets up folder paths

### 📦 Pack Type Selection  
- Choose which packs to process:
  - `premium`, `career`, `event`, `micro`, `npl`, `stage1_top_25k`

### 🧠 Automated Execution  
- Process all cohorts or selected ones  
- User ID extraction + BigQuery querying + revenue aggregation

### 🔍 Live Progress Monitoring  
- Real-time progress bar and cohort count  
- Logs displayed live as the pipeline runs  
- Runtime metrics (e.g. processing time, revenue so far)

### 📊 Visual Results  
- Bar chart: revenue by cohort  
- Pie chart: revenue by pack type  
- Line chart: trend over cohort days  
- Download results as Excel

### 🔐 Security Built-In  
- Password-protected UI (via Streamlit secrets)  
- No public exposure of credentials  
- GCP credentials securely managed via secret variables

## 📋 Data Inputs

### 📁 Folder Format

Uploaded `.zip` file should contain:
```
/1_June/
/2_June/
/3_June/
...
```

Each folder must contain some or all of the following files:
- `premium_packs_with_ad_ids.csv`
- `career_packs_with_ad_ids.csv`
- `event_packs_with_ad_ids.csv`
- `micro_packs_with_ad_ids.csv`
- `npl_packs_with_ad_ids.csv`
- `stage1_top_25k_with_ad_ids.csv`

Each CSV must have at least this column:  
→ `user_pseudo_id`

## 📡 Fetches Revenue From

**Google BigQuery table (example):**
```
wcc3-live.dataset_zero.Product_Table_Streaming
```

### Filters Used:
- Date range based on cohort start + window size
- `user_pseudo_id IN UNNEST([...])`
- USD → INR value conversion:
  - API: [Frankfurter](https://www.frankfurter.app)
  - OR fallback value: e.g. ₹86.191

## 📈 Example Outputs

### Revenue Summary Table (Sample)
| Cohort        | Premium Revenue | Event Revenue | Total Revenue |
|---------------|------------------|---------------|----------------|
| 1_June        | ₹1,00,000        | ₹85,000       | ₹1,85,000      |
| 2_June        | ₹95,000          | ₹60,000       | ₹1,55,000      |

### Visualizations
✅ Revenue by cohort  
✅ Revenue by pack type (pie chart)  
✅ Revenue trend across cohorts (line chart)  

## 🔐 Access Control

The app features two layers of security:

| Feature               | Description                                |
|-----------------------|--------------------------------------------|
| 🔐 Password Login     | Hidden behind a password field (set by you)|
| 🔑 GCP Credentials    | Managed via Streamlit secrets              |

These ensure that only authorized team members can access sensitive revenue information.

## 👤 Ideal Users

This dashboard is designed for:

- 📊 **Data Analysts**: Automate routine cohort revenue tracking
- 🎮 **Game Product Managers**: Review pack performance across activations
- 💼 **Finance Teams**: Convert USD revenue into INR daily
- 👨‍💻 **Developers**: Scale BigQuery cohort processing with clean pipelines

## 📦 Key Advantages

- ✅ Intuitive UI with minimal setup
- ✅ No code changes needed to run new cohorts
- ✅ Fast and scalable across large data sets
- ✅ Compatible with secure Streamlit deployment
- ✅ Easily export to Excel (.xlsx)

## 🧠 How It Works

1. Upload cohort ZIP
2. Choose packs
3. Save config
4. Run pipeline (all or selected cohorts)
5. Watch real-time logs and progress
6. View insights and download results
