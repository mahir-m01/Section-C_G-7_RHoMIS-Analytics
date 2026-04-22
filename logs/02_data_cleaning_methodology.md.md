# Data Extraction and Cleaning Methodology
**Project:** RHoMIS Analytics Capstone (Section C, Group 7)

---

## 1. Executive Summary
This document outlines the systematic extraction, transformation, and load (ETL) pipeline constructed for the Rural Household Multiple Indicator Survey (RHoMIS) dataset. The pipeline is designed to transform an exceptionally sparse, high-dimensional raw dataset into a robust analytical datamart optimized for Tableau visualizations and predictive modeling.

---

## 2. Phase 1: Data Extraction & Triage
The original dataset was provided as a compressed, mixed-type CSV (`raw.csv.gz`). Initial programmatic exploratory sweeps revealed a high degree of noise and sparsity.

### Raw Data Profile
- **Starting Dimensions:** 54,873 Rows × 1,599 Columns
- **File Footprint:** ~670 MB
- **Sparsity Crisis:** Over 1,417 columns (88%) registered null densities exceeding 50%.
- **Observation Strategy:** Rather than imputing vast numbers of inherently missing localized variables (e.g., specific micro-livestock parameters per village), the strategy was pivoted to **targeted extraction**. We isolated the core domain indicators vital to fulfilling the overarching business goals: Food Security and Household Income metrics.

### Targeted Column Selection
A subset of 60 crucial columns was retained, categorized into the following domains:
- **Identity & Geography:** `id_unique`, `country`, `iso_country_code`, `year`, `region`, `gps_lat_rounded`, `gps_lon_rounded`
- **Household Demographics:** `respondentsex`, `age_malehead`, `age_femalehead`, `education_level`, `respondent_is_head`, 8 age-band columns, `count_people`
- **Farm & Land:** `landcultivated`, `unitland`, `land_ownership`, `land_tenure`, `land_irrigated`, `farm_labour`
- **Crop Diversity & Production:** `crop_count`, `crop_name_1-5`, `crop_harvest_kg_per_year_1-3`, `crop_income_per_year_1-3`, `crop_land_area_1-3`, `crop_consumed_prop_1-3`
- **Income:** `local_currency`, `offfarm_incomes_any`, `offfarm_income_proportion`, `livestock_sale_income_1-2`
- **Food Security:** `hfias_1-9`, `fies_1-8`, `foodshortagetime`
- **Gender & Resource Control:** `land_ownership`, `crop_who_control_revenue_1-3`, `offfarm_who_control_revenue_1-2`, `livestock_meat_who_control_eating_1`, `dairy_products_who_control_eating`

---

## 3. Phase 2: Fundamental Data Cleaning Pipeline
With the dataset constrained to high-value features, a scripted Python ETL pipeline (`scripts/etl_pipeline.py`) was initiated to enforce data integrity.

### 3.1. Global String Cleaning
All string columns undergo a standardized cleaning pass:
- Strip leading/trailing whitespace
- Convert to lowercase
- Replace blank strings with NaN

This ensures consistency before domain-specific cleaning steps.

### 3.2. Country & Currency Standardization
- **Country names:** Fixed misspellings (e.g., `philipines` → `philippines`)
- **Currency:** Mapped CFA franc variants (`cfa_franc`, `fcfa_xof`, `fcfa`, `cfa`) to canonical `xof`; garbage entry `1` set to NaN

### 3.3. Outlier Rectification: The Age Phenomenon
- Initial descriptive statistics showcased massive anomalies where max values for `age_malehead` exceeded 19,000. 
- **The Finding:** Respondents incorrectly inputted their *Birth Years* (e.g., 1982) rather than their chronologic age.
- **The Fix:** An algorithmic rule was applied calculating `(survey_year - value)` where the value fell between 1900 and survey year. Following standard demographic practices, any remaining extreme mathematical impossibilities (>110 years or <0) were discarded as NaN.
- **Note:** Age cap is strict at 110 (not 120) to match ETL contract requirements.

### 3.4. Household Size Derivation
- `household_size_derived` computed as sum of 8 age-band columns (0% missing)
- All age-band columns coerced to numeric before summing
- Original `count_people` retained with NaN intact (no imputation per policy)

### 3.5. Standardizing Spatial Metrics (Area Units)
The survey operated globally, yielding diverse measurements such as Hectares, Acres, Timad, and others.
- **Unification Strategy:** The column `land_cultivated_ha` was instantiated. A conversion algorithm targeted string footprints in `unitland`. Where the string indicated "acre", a multiplier of `0.404686` was applied to convert to Hectares natively.
- **No Clipping Applied:** Unlike earlier versions, the pipeline does NOT clip extreme values at 1,500 ha. Unknown or unmappable units produce NaN instead, per the ETL contract.
- **landcultivated coercion:** Column coerced to numeric before row-wise conversion apply.

### 3.6. Binary Column Standardization
Binary fields (`offfarm_incomes_any`, `land_irrigated`, `respondent_is_head`, `foodshortagetime`) standardized to `y`/`n`/NaN:
- `y`, `yes`, `true`, `1` → `y`
- `n`, `no`, `false`, `0` → `n`
- Other values (no_answer, dont_know, etc.) → NaN

`respondentsex` mapped to `m`/`f`/NaN only.

---

## 4. Phase 3: Categorical Validation & Numeric Coercion

### 4.1. Crop Land Area & Consumed Proportion
- `crop_land_area_1-3`: Validated as categorical with allowed set `{little, underhalf, half, most, all, none}`; numeric contamination removed
- `crop_consumed_prop_1-3`: Validated as categorical with allowed set `{none, little, underhalf, half, most, all}`

### 4.2. Food Security Columns
- **HFIAS:** Kept `no_answer` as valid string value (not NaN)
- **FIES:** Mapped `yes`→`y`, `no`→`n`, retained `no_answer` as valid string

### 4.3. Income & Proportion Columns
- `offfarm_income_proportion`: Treated as categorical with allowed set `{little, underhalf, half, most, all, none}`
- All income/harvest columns coerced to numeric with `errors='coerce'`

### 4.4. Numeric Column Coercion
Columns coerced to numeric: `crop_count`, `gps_lat_rounded`, `gps_lon_rounded`, `count_people`, all income and harvest columns

### 4.5. Gender & Resource Control Columns
8 columns cleaned: `land_ownership`, `crop_who_control_revenue_1-3`, `offfarm_who_control_revenue_1-2`, `livestock_meat_who_control_eating_1`, `dairy_products_who_control_eating`
- Stripped, lowercased, `no_answer` and blank → NaN

---

## 5. Output & Export

### 5.1. Export Configuration
- **Output files:** 
  - `data/processed/rhomis_cleaned.csv`
  - `data/processed/rhomis_cleaned.csv.gz` (compressed)
- **Column count:** 60 columns (matching ETL REQUIRED_COLS)
- **No intermediate columns dropped:** HFIAS source strings, `unitland`, `landcultivated` all retained

### 5.2. No KPIs in Cleaning
The cleaning notebook produces **structural outputs only**. The following are deferred to analysis notebooks (03/04):
- `food_security_status`
- `income_per_capita`
- `farm_size_bucket`
- `hfias_total_score`
- `total_annual_income`

---

## Conclusion & Output Asset
The final processing script was executed over the raw dataset. The output was written to `data/processed/rhomis_cleaned.csv` and `data/processed/rhomis_cleaned.csv.gz`.

**Final Export Statistics:**
- **Final Rows:** 54,873 (zero attrition approach)
- **Final Columns:** 60 (across 7 domains including Gender & Resource Control)
- **Data Status:** Structurally cleaned, unit-standardized, categorically validated, and aligned with `scripts/etl_pipeline.py`. Ready for EDA (notebook 03), statistical analysis (notebook 04), and Tableau integration.

**Notebook-ETL Alignment:**
All notebook cleaning logic has been verified against the canonical ETL pipeline to ensure identical outputs. Key alignment points:
- Age cap at 110 (not 120)
- Acre conversion factor 0.404686 (not 0.4047)
- No land clipping at 1,500 ha
- FIES `no_answer` retained as string
- Export filename `rhomis_cleaned.csv` (not `cleaned_rhomis_data.csv`)
- Gender/resource-control columns included in load and export
