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
| **Faculty Mentor** | TBA |
| **Institute** | Newton School of Technology |
| **Submission Date** | TBA |

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

TBA — to be populated after column selection in `01_extraction`.

For full column definitions, see [`docs/data_dictionary.md`](docs/data_dictionary.md).

---

## KPI Framework

TBA

---

## Tableau Dashboard

TBA — see [`tableau/dashboard_links.md`](tableau/dashboard_links.md) once published.

---

## Key Insights

TBA — to be completed after analysis.

---

## Recommendations

TBA — to be completed after analysis.

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
5. **Final Load Prep** — Cleaned, analysis-ready CSV exported for Tableau (`05_final_load_prep`)
6. **Visualise** — Interactive Tableau dashboard published on Tableau Public
7. **Report** — Final report and presentation deck exported to `reports/`

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python + Jupyter Notebooks | ETL, cleaning, EDA, statistical analysis |
| Google Colab | Cloud notebook execution (supported) |
| Tableau Public | Dashboard design and publishing |
| GitHub | Version control and contribution audit |

**Python libraries:** `pandas`, `numpy`, `matplotlib`, `seaborn`, `scipy`, `missingno`

---

## Submission Checklist

**GitHub Repository**

- [ ] All notebooks committed in `.ipynb` format with outputs visible
- [ ] `data/raw/` contains the original, unedited dataset
- [ ] `data/processed/` contains the cleaned pipeline output
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
