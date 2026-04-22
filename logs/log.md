# Project Activity Log

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
- `02_data_cleaning_methodology.md.md`: Complete rewrite to reflect actual cleaning methodology (no KPIs, structural only)

---

## Project Notes
- ETL pipeline is canonical source of truth for cleaning logic
- Notebooks must align with ETL for reproducibility
- No KPIs in cleaning phase (deferred to notebooks 03/04)
- No imputation except household_size_derived from age bands