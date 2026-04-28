# RHoMIS Analytics — DVA Capstone 2

> **Newton School of Technology | Section C, Group 7 | Data Visualization & Analytics**
> A multi-country analysis of smallholder food insecurity using Python, GitHub, and Tableau — converting 54,873 raw survey records into actionable agricultural intelligence for policymakers and development organisations.

[![Tableau Public](https://img.shields.io/badge/Tableau-Published-blue?logo=tableau)](https://public.tableau.com/app/profile/rajdeep.sanyal/viz/RHoMISDASHBOARD/INCOMEPOSITION)
[![Dataset](https://img.shields.io/badge/Dataset-Harvard%20Dataverse-red)](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi%3A10.7910%2FDVN%2FWS38SA)
[![License](https://img.shields.io/badge/License-CC0%201.0-lightgrey)](https://creativecommons.org/publicdomain/zero/1.0/)

---

## Project Overview

| Field | Details |
|---|---|
| **Project Title** | Food Security Drivers in Smallholder Farming Households |
| **Sector** | Agriculture / Rural Development |
| **Team ID** | Section C, Group 7 |
| **Faculty Mentor** | Archit Raj |
| **Institute** | Newton School of Technology |
| **Submission Date** | 28 April 2026 |

### Team Members & Roles

| Name | GitHub | Role |
|---|---|---|
| Mahir Abdullah | [`mahir-m01`](https://github.com/mahir-m01) | Team, Analysis & Report Lead |
| Manas Vivek Saxena | [`ManasSaxena14`](https://github.com/ManasSaxena14) | ETL & PPT Lead |
| Manya Verma | [`manyaverma727`](https://github.com/manyaverma727) | Data & ETL |
| Rajat Srivastav | [`rajatrsrivastav`](https://github.com/rajatrsrivastav) | Data & ETL |
| Rajdeep Sanyal | [`rajdeep-2004`](https://github.com/rajdeep-2004) | Visualisation Lead |
| Sankalp | [`Sankalp13353`](https://github.com/Sankalp13353) | Strategy & Statistical Analysis |

---

## Business Problem

Smallholder farming households across 35 countries face chronic food insecurity and low farm income. Despite existing development programmes, intervention targeting remains imprecise because the structural drivers of vulnerability — irrigation access, land productivity, income position, education, and household composition — vary significantly by region and farm profile.

**Core Business Question**

> Which regions and farm profiles are most vulnerable to food insecurity and low income — and what structural factors drive these outcomes?

**Decision Supported**

> Policymakers, NGOs, and development organisations can use this analysis to identify where to direct irrigation infrastructure, productivity support, and income diversification programmes — maximising impact per household reached.

---

## Dataset

| Attribute | Details |
|---|---|
| **Source** | RHoMIS (Rural Household Multi-Indicator Survey) |
| **Published By** | Harvard Dataverse — Gorman, Hammond, Frelat, Caulfield et al. |
| **DOI / Access** | [10.7910/DVN/WS38SA](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi%3A10.7910%2FDVN%2FWS38SA) |
| **Version** | Version 1 — January 30, 2025 |
| **License** | CC0 1.0 (Public Domain) |
| **Raw Rows** | 54,873 farm households |
| **Raw Columns** | 1,599 survey variables |
| **Working Columns** | 77 (ETL selection) → 46 (Tableau-ready export) |
| **Countries** | 35 across Sub-Saharan Africa, Latin America, and Asia |
| **Period** | 2015 – 2023 |

**Column Groups Used**

| Group | Key Columns |
|---|---|
| Identity & Geography | `id_unique`, `country`, `iso_country_code`, `year`, `region`, `gps_lat_rounded`, `gps_lon_rounded` |
| Demographics | `respondentsex`, `respondent_is_head`, `education_level`, `age_malehead`, `age_femalehead`, `count_people`, 8 age-band columns |
| Derived | `household_size_derived` (sum of age bands), `land_cultivated_ha` (unit-converted) |
| Farm & Land | `landcultivated`, `unitland`, `land_tenure`, `land_ownership`, `land_irrigated`, `farm_labour` |
| Crops & Production | `crop_name_1–5`, `crop_harvest_kg_per_year_1–3`, `crop_income_per_year_1–3`, `crop_consumed_prop_1–3` |
| Income | `offfarm_incomes_any`, `offfarm_income_proportion`, `livestock_sale_income_1–2` |
| Food Security | `foodshortagetime`, `fies_1–8`, `hfias_1–9` |
| Gender & Resource Control | `crop_who_control_revenue_1–3`, `offfarm_who_control_revenue_1–2`, `livestock_meat_who_control_eating_1` |

Full column definitions: [`docs/data_dictionary.md`](docs/data_dictionary.md)

---

## Analytical Pipeline

```
Raw Survey (54,873 × 1,599)
        │
        ▼
01_extraction.ipynb        — Load, validate, select 60 working columns
        │
        ▼
02_cleaning.ipynb          — Structural cleaning (mirrors ETL script)
scripts/etl_pipeline.py    ← Canonical cleaning implementation
        │
        ▼
03_eda.ipynb               — Distribution, structural drivers, gender analysis
        │
        ▼
04_statistical_analysis.ipynb  — Chi-square, Mann-Whitney, logistic regression, k-means
        │
        ▼
05_final_load_prep.ipynb   — KPI derivation, Tableau-ready CSV export
        │
        ▼
Tableau Dashboard          — 4 interactive views on Tableau Public
        │
        ▼
Final Report PDF           — reports/RHoMIS-report.pdf
```

---

## KPI Framework

Seven KPIs form the dashboard backbone, each directly connected to a project recommendation.

| KPI | Value | Formula |
|---|---|---|
| Food Shortage Rate | **67.7%** | `AVG(food_shortage_flag)` |
| Food-Shortage Household Count | **32,097** | `COUNT WHERE food_shortage_flag = 1` |
| Median Months of Shortage | **3.0 mo** | `MEDIAN(foodshortagetime)` |
| Irrigation Access Rate | **20.2%** | `AVG(irrigated_flag)` |
| Median Land Productivity | **600 kg/ha** | `MEDIAN(land_productivity_kg_per_ha)` |
| Median Income per MAE | **$112 PPP** | `MEDIAN(income_ppp_per_mae)` |
| Shortage Rate — Lowest Quintile | **75.8%** | `AVG(food_shortage_flag) WHERE income_quintile = 1` |

> **No imputation applied.** Missing values remain `NaN` throughout. Missingness in RHoMIS is structural — imputation would misrepresent the survey design.

---

## Tableau Dashboard

**Published:** [RHoMIS Dashboard on Tableau Public](https://public.tableau.com/app/profile/rajdeep.sanyal/viz/RHoMISDASHBOARD/INCOMEPOSITION)

| View | Description | Link |
|---|---|---|
| Risk Overview | Choropleth map, food shortage rate by country, median months of shortage | [Open](https://public.tableau.com/app/profile/rajdeep.sanyal/viz/RHoMISDASHBOARD/RISKOVERVIEWMAP) |
| Vulnerability Profiles | Shortage by k-means profile × country, demographic heatmap | [Open](https://public.tableau.com/app/profile/rajdeep.sanyal/viz/RHoMISDASHBOARD/VULNERABILITYPROFILES) |
| Structural Drivers | Productivity quartile × irrigation, crop diversity, land scatter | [Open](https://public.tableau.com/app/profile/rajdeep.sanyal/viz/RHoMISDASHBOARD/STRUCTURALDRIVERS) |
| Income Position | Income rank vs. food shortage scatter, off-farm income by profile, quintile distribution | [Open](https://public.tableau.com/app/profile/rajdeep.sanyal/viz/RHoMISDASHBOARD/INCOMEPOSITION) |

Screenshots: [`tableau/screenshots/`](tableau/screenshots/)

---

## Key Findings

1. **Food insecurity is the structural norm.** 67.7% of surveyed households (32,097 of 47,399 with valid responses) reported food shortage — confirming this is not an exception but a baseline condition across smallholder systems.

2. **Irrigation is the clearest addressable gap.** Irrigated households face a 55.1% shortage rate vs. 72.4% for rainfed — a **17.4pp difference** — yet only 20.2% of households have irrigation access.

3. **Land productivity quartile creates a 29.1pp spread.** Q1 households (lowest productivity) face 83% shortage vs. 54% for Q4. This gap is independent of country-level poverty and income.

4. **Income position is the strongest regression predictor.** A logistic regression controlling for country fixed effects finds income rank is the top predictor (OR 0.456, p < 0.001). Low education (OR 0.71) and large household size compound every other risk factor.

5. **Geographic concentration in Central and West Africa.** DRC (90.7%), Comoros (87.0%), Malawi (85.2%), and Burundi lead shortage rates. India (10.5%) and Costa Rica (18.6%) anchor the low end — an 84pp country-level spread.

6. **Female-headed households face a consistent additional penalty.** Chi-square tests confirm statistically significant differences (p < 0.001) in food shortage rates and resource control patterns across gender.

7. **The low-income rainfed k-means cluster (14,330 HH) has 78.7% shortage and 0.3% irrigation coverage** — the single highest-return intervention target.

---

## Recommendations

| Priority | Recommendation | Evidence | Estimated Effect |
|---|---|---|---|
| 1 | **Irrigation expansion** in the low-income rainfed cluster | 17.4pp gap; 0.3% cluster coverage | −17.4pp shortage per HH reached |
| 2 | **Productivity support** for Q1 + Q2 farms (24,000 HH) | 29.1pp Q1–Q4 spread | −10 to 15pp shortage |
| 3 | **Income diversification + education** for bottom quintile | 75.8% shortage rate in lowest quintile | −4 to 8pp shortage |

---

## Repository Structure

```text
Section-C_G-7_RHoMIS-Analytics/
│
├── README.md
│
├── data/
│   ├── raw/                              # Original dataset (gitignored — never committed)
│   └── processed/
│       ├── rhomis_cleaned.csv            # 54,873 rows × 77 columns
│       ├── rhomis_cleaned.csv.gz         # Compressed copy
│       ├── rhomis_cleaning_summary.json  # ETL run metadata
│       └── rhomis_tableau_ready.csv      # 54,873 rows × 46 columns (Tableau export)
│
├── notebooks/
│   ├── 01_extraction.ipynb               # Load, validate, select columns
│   ├── 02_cleaning.ipynb                 # Structural cleaning — mirrors ETL
│   ├── 03_eda.ipynb                      # Distribution, drivers, gender analysis
│   ├── 04_statistical_analysis.ipynb     # Regression, chi-square, k-means
│   └── 05_final_load_prep.ipynb          # KPI derivation, Tableau CSV export
│
├── scripts/
│   └── etl_pipeline.py                   # Canonical ETL implementation (source of truth)
│
├── tableau/
│   ├── RHoMIS DASHBOARD - DVA CAPSTONE.twbx  # Tableau workbook
│   ├── dashboard_links.md                     # Tableau Public URLs
│   └── screenshots/                           # All 4 dashboard views
│
├── reports/
│   ├── project_report.tex                # LaTeX source
│   └── RHoMIS-report.pdf                 # Final compiled report
│
├── docs/
│   └── data_dictionary.md                # Full column definitions (46 fields)
│
└── logs/
    └── log.md                            # Project activity log
```

---

## Tech Stack

| Tool | Purpose |
|---|---|
| **Python 3.x** | ETL, EDA, statistical analysis, KPI derivation |
| **Jupyter Notebooks** | Analysis documentation and reproducibility |
| **pandas, numpy** | Data manipulation and transformation |
| **matplotlib, seaborn** | Visualisation in notebooks |
| **scipy, statsmodels** | Chi-square, Mann-Whitney, logistic regression |
| **scikit-learn** | k-means clustering (vulnerability profiles) |
| **Tableau Public** | Interactive dashboard — 4 views, published |
| **GitHub** | Version control, contribution audit trail |
| **LaTeX / Overleaf** | Final academic report |

---

## Submission Checklist

**GitHub Repository**
- [x] All 5 notebooks committed in `.ipynb` format with outputs visible
- [x] `data/processed/` contains cleaned pipeline output and Tableau-ready export
- [x] `tableau/screenshots/` contains all 4 dashboard view screenshots
- [x] `tableau/dashboard_links.md` contains the Tableau Public URL
- [x] `docs/data_dictionary.md` complete with all 46 field definitions
- [x] All team members have visible commits

**Tableau**
- [x] Dashboard published on Tableau Public (public URL active)
- [x] Multiple interactive filters included (Country, Year, Irrigation Flag, Vulnerability Profile, Gender)
- [x] Dashboard directly addresses the business problem across 4 views

**Report**
- [x] Final report compiled as PDF — `reports/RHoMIS-report.pdf`

---

## Contribution Matrix

| Team Member | Dataset & Sourcing | ETL & Cleaning | EDA & Analysis | Statistical Analysis | Tableau Dashboard | Report Writing | PPT & Viva |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Mahir Abdullah | ✓ | | ✓ | ✓ | ✓ | ✓ | |
| Manas Vivek Saxena | ✓ | ✓ | | | | | ✓ |
| Manya Verma | ✓ | ✓ | | | | | |
| Rajat Srivastav | ✓ | ✓ | | | | | |
| Rajdeep Sanyal | ✓ | | | | ✓ | | |
| Sankalp | ✓ | | | ✓ | | | |

_Declaration: We confirm that the above contribution details are accurate and verifiable through GitHub Insights, PR history, and submitted artifacts._

---

## Academic Integrity

All analysis, code, and recommendations in this repository are the original work of the team listed above. Contributions are fully traceable via GitHub commit history and pull request audit trail.

---

*Newton School of Technology — Data Visualization & Analytics | DVA Capstone 2 | 28 April 2026*
