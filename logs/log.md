# Project Activity Log

## 2026-04-28: Tableau Dashboard Published — Report & Repository Finalized

### Summary
Completed all remaining submission deliverables. The Tableau dashboard is fully published on Tableau Public across four interactive views. The final academic report has been compiled to PDF. The repository has been cleaned, synced with all remote branches, and the README updated to reflect the final project state.

### Tableau Dashboard — Completed
- All four dashboard views designed, refined, and published on Tableau Public by Rajdeep Sanyal.
- **Risk Overview:** Choropleth world map, food shortage rate by country, median months of shortage by country.
- **Vulnerability Profiles:** Food shortage rate by k-means profile × country, profile distribution pie, demographic heatmap (irrigation rate, education, off-farm income by profile).
- **Structural Drivers:** Productivity quartile × irrigation type matrix, crop diversity × shortage rate chart, land productivity vs. food shortage scatter (country-level).
- **Income Position:** Income rank vs. food shortage scatter, off-farm income by vulnerability profile, income quintile distribution by country.
- Published URL: https://public.tableau.com/app/profile/rajdeep.sanyal/viz/RHoMISDASHBOARD/INCOMEPOSITION
- Tableau workbook committed to repository: `tableau/RHoMIS DASHBOARD - DVA CAPSTONE.twbx`
- All four dashboard screenshots committed to `tableau/screenshots/`.

### Key Dashboard Metrics (verified against published views)
- Food Shortage Rate: 67.7% (32,097 of 47,399 valid households)
- Median Shortage Duration: 3.0 months
- Irrigation Advantage: 17.4pp (rainfed 72.4% vs. irrigated 55.1%)
- Median Land Productivity: 600 kg/ha
- Median Income per MAE: $112 PPP
- Shortage Rate — Lowest Income Quintile: 75.8%

### Final Report — Compiled
- LaTeX source finalized: `reports/project_report.tex`
- Report compiled to PDF: `reports/RHoMIS-report.pdf`
- All KPI values, irrigation figures, and team roles verified against dashboard outputs.
- Contribution matrix and declaration section included per rubric requirements.

### Repository Cleanup & Sync
- Removed duplicate image assets from `reports/` (canonical copies retained in `.ai-workflow/images/`).
- Local `dev` branch rebased on `origin/dev` — all remote changes merged cleanly.
- `main` branch fast-forwarded 32 commits to match `origin/main`.
- `tableau/` folder reset to match repository state exactly.
- README rewritten to reflect final project state, team roles, live Tableau URLs, and completed submission checklist.

### Verification
- `git status`: clean working tree on `dev` (only untracked `reports/RHoMIS-report.pdf`).
- All 5 notebooks committed with executed outputs visible.
- `data/processed/`: `rhomis_cleaned.csv`, `rhomis_cleaned.csv.gz`, `rhomis_cleaning_summary.json`, `rhomis_tableau_ready.csv` — all present.
- `tableau/dashboard_links.md`: updated with all four Tableau Public view URLs.
- Submission checklist: all items complete.

---

## 2026-04-24: Final Load Preparation and Tableau Export

### Summary
Completed the final load preparation workflow for the analysis phase and produced the Tableau-ready export based on the verified outputs from notebooks 03 and 04.

### Notebook 05 — Final Load Preparation
- Rebuilt the approved derived analysis fields required for dashboarding.
- Added a final KPI framework with clear definitions, formulas, valid populations, and cautions.
- Added coverage and validation checks before export.
- Exported the final Tableau-ready dataset for dashboard development.

### Final Dashboard Fields
- Headline vulnerability outcome: food shortage flag and months of food shortage.
- Core structural drivers: irrigation access, cultivated land, crop diversity, and land productivity.
- Normalized welfare measures: PPP-adjusted income per MAE and within-country income rank.
- Supporting food-security and nutrition measures: FIES, HFIAS, and HDDS indicators.
- Vulnerability segmentation: interpretable household profiles for dashboard drill-down.

### Notebook Presentation Polish
- Added clear markdown explanations before every code cell in notebooks 03, 04, and 05.
- Kept evaluator-facing wording professional and removed dependency on internal workflow notes.
- Retained executed outputs in all three notebooks for reviewability.

### Verification
- `03_eda.ipynb`: all code cells executed and presentation-ready.
- `04_statistical_analysis.ipynb`: all code cells executed and presentation-ready.
- `05_final_load_prep.ipynb`: executed successfully and exported final Tableau-ready CSV.
- Final Tableau-ready dataset shape: 54,873 rows × 46 columns.
- `logs/` continues to contain only this `log.md` file.

## 2026-04-23: EDA and Statistical Analysis Finalization

### Summary
Finalized the analysis phase notebooks and prepared a clean handoff for the KPI and Tableau-load phase.

### Notebook 03 — EDA
- Built and executed `03_eda.ipynb`.
- Added pre-analysis checks for valid populations, missingness, currency issues, and indicator coverage.
- Created defensible analysis fields for food shortage, crop diversity, land productivity, PPP-normalized income position, FIES, and HFIAS.
- Kept the notebook focused on exploration and candidate dashboard metrics, not final KPI ownership.

### Notebook 04 — Statistical Analysis
- Built and executed `04_statistical_analysis.ipynb`.
- Added hypothesis tests, robust numeric comparisons, correlation checks, logistic regression, and vulnerability segmentation.
- Confirmed key supported drivers: land productivity, land scale, education, income position, and irrigation.
- Kept results framed as association, not causation.

### Notebook 05 — Final Load Prep Template
- Replaced full export logic with a lightweight template.
- Added placeholders for final KPI framework, dashboard-field rebuild, final field selection, and export.
- No Tableau CSV is exported yet; final export is pending KPI completion.

### Verification
- `03_eda.ipynb`: 29 cells, 15 executed code cells.
- `04_statistical_analysis.ipynb`: 26 cells, 13 executed code cells.
- `05_final_load_prep.ipynb`: 12 cells, 6 executed code cells.
- Evaluator-facing notebooks do not contain external methodology links.
- `logs/` contains only this `log.md` file.

---

## 2026-04-22: Notebook-ETL Alignment (Commit ef2bbb7)

### Summary
Applied 26 fixes to align `01_extraction.ipynb` and `02_cleaning.ipynb` with canonical ETL pipeline (`scripts/etl_pipeline.py`).

### Changes in 01_extraction.ipynb
- Fixed column name: `landowned` → `land_ownership`
- Added Gender & Resource Control group (8 columns)
- Added WORKING_COLS cell

### Changes in 02_cleaning.ipynb

**Data Loading:**
- Added `usecols` parameter to `pd.read_csv`
- Added 8 gender/resource-control columns to `columns_to_keep`
- Removed `crop_harvest_1` and `crop_yield_1` from loading

**String Cleaning:**
- Added global clean_strings pass (strip, lowercase, blank→NaN)
- Removed redundant inline string operations

**Age Processing:**
- Fixed age cap: 120 → 110 (ETL strict cap)

**Household Size:**
- Added numeric coercion for age-band columns before summing

**Land Conversion:**
- Fixed acre factor: 0.4047 → 0.404686
- Removed land clipping at 1,500 ha
- Added landcultivated numeric coercion

**Binary Columns:**
- Moved `foodshortagetime` to binary_fields

**Crop Validation:**
- Added crop_consumed_prop_1/2/3 categorical validation
- Added numeric coercion for crop_count, gps_lat/lon, count_people

**Food Security:**
- Fixed FIES no_answer handling (retain as string, not NaN)

**Income:**
- Added offfarm_income_proportion categorical cleaning

**Export:**
- Fixed filename: `cleaned_rhomis_data.csv` → `rhomis_cleaned.csv`
- Added gender columns to export
- Removed crop_harvest_1/crop_yield_1 from export
- Added gzip compressed export

**Documentation:**
- Updated intro: structural cleaning only, no KPIs
- Moved export markdown before code cell
- Updated export and imputation text

### Verification
All 26 fixes verified against ETL pipeline. Notebooks now produce identical output to `scripts/etl_pipeline.py`.

### Logs Updated
- `01_data_summary.md`: Corrected column references, updated next steps
- `02_data_cleaning_methodology.md.md`: Complete rewrite to reflect actual cleaning methodology (77 columns, no KPIs, structural only)

---

## 2026-04-22: Data Cleaning & ETL Pipeline Rewrite (Commit 8cb40fd)

### Summary
Complete rewrite of the ETL pipeline (`scripts/etl_pipeline.py`) to align with the core cleaning contract. The pipeline now serves as the canonical source of truth for all data transformations.

### Key Enhancements
- **Extraction:** Optimized loading using `usecols` (75 required columns) for memory efficiency.
- **Spatial Metrics:** Integrated a comprehensive `UNIT_HA` conversion table supporting diverse units (timad, ares, manzanas, bigha, etc.).
- **Demographics:** 
  - Dynamic age correction using survey year column (not hardcoded).
  - Strict age plausibility ceiling at 110 years.
  - Derived `household_size_derived` from 8 age-band columns with 0% missingness.
- **Integrity:**
  - No imputation policy enforced (except derived household size).
  - NaN values preserved for income and `count_people`.
  - HFIAS/FIES exported as cleaned strings.
- **Gender & Resource Control:** Included 8 specialized columns for revenue and eating control analysis.

### Final Output Statistics
- **Total Rows:** 54,873 (Zero attrition approach)
- **Total Columns:** 77 (across 7 domains)
- **Files Generated:** `rhomis_cleaned.csv`, `rhomis_cleaned.csv.gz`, audit log, and summary JSON.

### Verification
Pipeline output verified for structural integrity. Successfully handles global unit variations and demographic anomalies.

---

## Project Notes
- ETL pipeline is canonical source of truth for cleaning logic
- Notebooks must align with ETL for reproducibility
- No KPIs in cleaning phase (deferred to notebooks 03/04)
- No imputation except household_size_derived from age bands
