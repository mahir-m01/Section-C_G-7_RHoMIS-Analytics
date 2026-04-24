# RHoMIS Analytics — DVA Capstone 2

> **Newton School of Technology | Data Visualization & Analytics**
> A 2-week industry simulation capstone using Python, GitHub, and Tableau to convert raw survey data into actionable agricultural intelligence.

---

## Project Overview

| Field | Details |
|---|---|
| **Project Title** | RHoMIS Analysis |
| **Sector** | Agriculture / Rural Development |
| **Team ID** | G-7 |
| **Section** | C |
| **Faculty Mentor** | Archit Raj |
| **Institute** | Newton School of Technology |
| **Submission Date** | April 2026 |

### Team Members

| Name | GitHub Username |
|---|---|
| Mahir Abdullah | `mahir-m01` |
| Manas Vivek Saxena | `ManasSaxena14` |
| Manya Verma | `manyaverma727` |
| Rajat Srivastav | `rajatrsrivastav` |
| Rajdeep Sanyal | `rajdeep-2004` |
| Sankalp | `Sankalp13353` |

---

## Business Problem

Smallholder farming households across 35 countries face chronic food insecurity and low farm income. Which regions are most at risk, and what combination of farm size, crop diversity, land productivity, household demographics, and other factors drive these outcomes?

**Core Business Question**

> Which regions and farm profiles are most vulnerable to food insecurity and low income — and what factors drive these outcomes?

**Decision Supported**

> Farmers, policymakers, and development organisations can identify which regions and farm profiles are most vulnerable and decide whether to act on income support, crop diversification, land productivity improvements, or targeted food assistance programmes.

---

## Dataset

> **Full title:** The Rural Household Multi-Indicator Survey (RHoMIS) data of 54,873 farm households in 35 countries

| Attribute | Details |
|---|---|
| **Source Name** | RHoMIS (Rural Household Multi-Indicator Survey) |
| **Published By** | Harvard Dataverse — Gorman, Hammond, Frelat, Caulfield et al. (76 contributors) |
| **Direct Access Link** | [DOI: 10.7910/DVN/WS38SA](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi%3A10.7910%2FDVN%2FWS38SA) |
| **Version** | Version 1 — January 30, 2025 |
| **License** | CC0 1.0 (Public Domain) |
| **Row Count** | 54,873 farm households |
| **Column Count** | 1,599 raw survey variables |
| **Countries Covered** | 35 across Latin America, Africa, and Asia |
| **Time Period Covered** | 2015 – 2023 |
| **Format** | CSV |

**Key Columns Used**

The ETL working set currently exports 77 cleaned columns from the 1,599-column raw survey.

- Identity & geography: `id_unique`, `country`, `iso_country_code`, `year`, `region`, `gps_lat_rounded`, `gps_lon_rounded`
- Demographics: `respondentsex`, `respondent_is_head`, `education_level`, `age_malehead`, `age_femalehead`, `count_people`, 8 household composition columns
- Derived structural columns: `household_size_derived`, `land_cultivated_ha`
- Farm & land: `landcultivated`, `unitland`, `land_tenure`, `land_ownership`, `land_irrigated`, `farm_labour`
- Crops & production: `crop_count`, `crop_name_1` to `crop_name_5`, `crop_harvest_kg_per_year_1` to `_3`, `crop_income_per_year_1` to `_3`, `crop_land_area_1` to `_3`, `crop_consumed_prop_1` to `_3`
- Income diversification: `local_currency`, `offfarm_incomes_any`, `offfarm_income_proportion`, `livestock_sale_income_1`, `livestock_sale_income_2`
- Food security: `foodshortagetime`, `fies_1` to `fies_8`, `hfias_1` to `hfias_9`
- Gender & resource control: `crop_who_control_revenue_1` to `_3`, `offfarm_who_control_revenue_1` to `_2`, `livestock_meat_who_control_eating_1`, `dairy_products_who_control_eating`

For full column definitions, see [`docs/data_dictionary.md`](docs/data_dictionary.md).

---

## KPI Framework

The final KPI layer is built in `05_final_load_prep.ipynb` using the verified outputs of notebooks `03_eda.ipynb` and `04_statistical_analysis.ipynb`.

Current final KPI set:

- Food Shortage Rate: `AVG(food_shortage_flag)`
- Food-Shortage Household Count: `COUNTD(IF [food_shortage_flag] = 1 THEN [id_unique] END)`
- Median Months of Food Shortage: `MEDIAN([nr_months_food_shortage])`
- Irrigation Access Rate: `AVG([irrigated_flag])`
- Median Land Productivity: `MEDIAN([land_productivity_kg_per_ha])`
- Median Income per MAE (PPP): `MEDIAN([income_ppp_per_mae])`
- Lowest-Income Household Share: `AVG(IF [country_income_rank_pct] <= 0.20 THEN 1 ELSE 0 END)`
- Mean FIES Score: `AVG([fies_yes_count])`

Important limitations:

- Raw local-currency income is not used as a dashboard KPI.
- FIES and HFIAS are subset metrics and must be labeled with their valid populations.

---

## Tableau Dashboard

The Tableau-ready dataset is exported by `05_final_load_prep.ipynb` to `data/processed/rhomis_tableau_ready.csv`.

Planned dashboard views are documented in [`tableau/dashboard_links.md`](tableau/dashboard_links.md). The Tableau Public URL and screenshots will be added once the dashboard is published.

---

## Key Insights

Current verified analysis highlights:

- Cleaned ETL output contains 54,873 rows and 77 columns.
- Final Tableau-ready dataset contains 54,873 rows and 46 columns.
- `foodshortagetime` remains the strongest full-dataset food-vulnerability outcome with 47,399 valid households.
- FIES provides a strong supporting severity layer on 25,072 households, while HFIAS remains a smaller supporting subset at 6,847 households.
- Households without irrigation show materially worse food-shortage outcomes than irrigated households.
- Land productivity is one of the strongest structural drivers of food shortage.
- PPP-adjusted income per MAE and within-country income rank are safer income measures than raw local-currency comparisons.
- The final vulnerability segmentation identifies four interpretable household profiles for Tableau drill-down.

---

## Recommendations

- Prioritize food shortage as the headline vulnerability outcome in Tableau.
- Use land productivity, irrigation access, and normalized income position as the main decision drivers.
- Label FIES and HFIAS clearly as subset-based supporting metrics.
- Avoid raw cross-country comparisons using local-currency income.
- Use the vulnerability profiles for regional targeting and household drill-down.

---

## Repository Structure

```text
Section-C_G-7_RHoMIS-Analytics/
|
|-- README.md
|
|-- data/
|   |-- raw/                         # Original dataset (never edited — gitignored)
|   `-- processed/                   # Cleaned output from ETL pipeline
|
|-- notebooks/
|   |-- 01_extraction.ipynb
|   |-- 02_cleaning.ipynb
|   |-- 03_eda.ipynb
|   |-- 04_statistical_analysis.ipynb
|   `-- 05_final_load_prep.ipynb
|
|-- scripts/
|   `-- etl_pipeline.py
|
|-- tableau/
|   |-- screenshots/
|   `-- dashboard_links.md
|
|-- reports/
|   |-- project_report.pdf
|   `-- presentation.pdf
|
`-- docs/
    `-- data_dictionary.md
```

---

## Analytical Pipeline

1. **Extract** — Raw dataset loaded and validated; data dictionary drafted (`01_extraction`)
2. **Clean & Transform** — Column selection, missing value handling, standardisation, feature engineering (`02_cleaning`)
3. **EDA** — Missing value analysis, distribution plots, cross-variable exploration (`03_eda`)
4. **Statistical Analysis** — Correlation, segmentation, and gap analysis (`04_statistical_analysis`)
5. **Final Load Prep** — KPI framework and Tableau-ready CSV exported (`05_final_load_prep`)
6. **Visualise** — Interactive Tableau dashboard published on Tableau Public
7. **Report** — Final report and presentation deck exported to `reports/`

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python + Jupyter Notebooks | ETL, cleaning, EDA, statistical analysis, final dashboard load prep |
| Google Colab | Cloud notebook execution (supported) |
| Tableau Public | Dashboard design and publishing |
| GitHub | Version control and contribution audit |

**Python libraries:** `pandas`, `numpy`, `matplotlib`, `seaborn`, `scipy`, `statsmodels`, `scikit-learn`

---

## Submission Checklist

**GitHub Repository**

- [ ] All notebooks committed in `.ipynb` format with outputs visible
- [ ] `data/raw/` contains the original, unedited dataset
- [x] `data/processed/` contains the cleaned pipeline output and Tableau-ready export
- [ ] `tableau/screenshots/` contains dashboard screenshots
- [ ] `tableau/dashboard_links.md` contains the Tableau Public URL
- [ ] `docs/data_dictionary.md` is complete
- [ ] All members have visible commits

**Tableau**

- [ ] Published on Tableau Public with public URL
- [ ] At least one interactive filter included
- [ ] Dashboard directly addresses the business problem

**Report**

- [ ] Final report exported as PDF into `reports/`
- [ ] Final presentation exported as PDF into `reports/`

---

## Contribution Matrix

| Team Member | Dataset & Sourcing | ETL & Cleaning | EDA & Analysis | Statistical Analysis | Tableau Dashboard | Report Writing | PPT & Viva |
|---|---|---|---|---|---|---|---|
| Mahir Abdullah | | | | | | | |
| Manas Vivek Saxena | | | | | | | |
| Manya Verma | | | | | | | |
| Rajat Srivastav | | | | | | | |
| Rajdeep Sanyal | | | | | | | |
| Sankalp | | | | | | | |

_Declaration: We confirm that the above contribution details are accurate and verifiable through GitHub Insights, PR history, and submitted artifacts._

---

## Academic Integrity

All analysis, code, and recommendations in this repository are the original work of the team listed above. Contributions are tracked via GitHub Insights and pull request history.

---

*Newton School of Technology — Data Visualization & Analytics | Capstone 2*
