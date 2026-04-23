# Data Dictionary

**Dataset:** RHoMIS (Rural Household Multi-Indicator Survey)  
**Source:** Harvard Dataverse — [DOI: 10.7910/DVN/WS38SA](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi%3A10.7910%2FDVN%2FWS38SA)  
---

## Files

| File | Description | Location |
|---|---|---|
| `rhomis_full_survey_data.csv` | Raw survey responses — 54,873 rows × 1,599 cols | `data/raw/` |
| `rhomis_cleaned.csv.gz` | Canonical cleaned ETL output — 54,873 rows × 77 cols | `data/processed/` |
| `rhomis_cleaning_summary.json` | ETL run summary, output schema, and missing-value counts | `data/processed/` |

---

## Key Column Groups (working subset — 77 exported columns)

### Household Demographics

`respondentsex`, `respondent_is_head`, `education_level`, `age_malehead`, `age_femalehead`, `count_people`, `children_under_4`, `children_4to10`, `males11to24`, `females11to24`, `males25to50`, `females25to50`, `malesover50`, `femalesover50`, `household_size_derived`

### Income & Productivity

`local_currency`, `crop_income_per_year_1` to `_3`, `livestock_sale_income_1` to `_2`, `offfarm_incomes_any`, `offfarm_income_proportion`, `landcultivated`, `unitland`, `land_cultivated_ha`, `crop_harvest_kg_per_year_1` to `_3`

### Food Security

`foodshortagetime`, `fies_1` to `fies_8`, `hfias_1` to `hfias_9`

### Gender Control Variables

`land_ownership`, `crop_who_control_revenue_1` to `_3`, `offfarm_who_control_revenue_1` to `_2`, `livestock_meat_who_control_eating_1`, `dairy_products_who_control_eating`

### Geography & Identity

`id_unique`, `country`, `iso_country_code`, `year`, `region`, `gps_lat_rounded`, `gps_lon_rounded`

### Farm Structure

`land_tenure`, `land_irrigated`, `farm_labour`, `crop_count`, `crop_name_1` to `_5`, `crop_land_area_1` to `_3`, `crop_consumed_prop_1` to `_3`

---

## Derived Columns From Cleaning

| Column | Formula | Notes |
|---|---|---|
| `household_size_derived` | Sum of the 8 age-band columns | Replaces missing `count_people` for structural analysis; no imputation was applied |
| `land_cultivated_ha` | `landcultivated` converted using `unitland` | Unknown or invalid land units are retained as `NaN` |

---

## Notes

- Canonical cleaning logic lives in `scripts/etl_pipeline.py`
- No imputation was performed, except the structural derivation of `household_size_derived` from complete age-band columns
- HFIAS and FIES are retained as cleaned string responses, not scored indicators
- KPI-style fields such as `food_security_status`, `income_per_capita`, `total_annual_income`, and `farm_size_bucket` are analysis-phase outputs and do not belong in cleaning
- Income columns remain in local currency and are not directly comparable across countries without additional normalization or conversion
