# Data Analysis Summary

## Overview
This document summarizes the insights derived from the initial data extraction of the RHoMIS dataset (`raw.csv.gz`).

### Basic Statistics
- **Total Rows**: 54,873
- **Total Columns**: 1,599
- **Duplicate Rows**: 0
- **Total Memory Usage**: ~669.4+ MB

### Data Quality Issues Detected
1. **Extensive Sparsity**: Out of 1599 columns, **1417 columns** have more than 50% missing values.
2. **Missing Outliers**: A few columns, such as `female_adult_bees_honey_sold_kg_per_year_2`, `bees_who_sells_4`, and `x_tags`, are 100% empty.
3. **Data Types**: The dataset contains mixed data types (float64, int64, object). Downstream cleaning will require mapping specific object columns to strictly categorical or numeric data types to enable modeling.

### Core Column Groups (Sample)

#### Demographics
`iso_country_code`, `country`, `region`, `respondentsex`, `age_malehead`, `age_femalehead`, `count_people`

#### Land & Tenure
`land_tenure`, `landcultivated`, `unitland`, `land_ownership`

#### Crop Production
`crop_count`, `crop_name_1` to `crop_name_5`, `crop_harvest_kg_per_year_1/2/3`, `crop_income_per_year_1/2/3`

#### Food Security & Nutrition
`hfias_1` to `hfias_9` (populated between 12-30%), `crop_consumed_prop_1`, `crop_calories_consumed_kcal_1`

### Next Steps 
The next phase (`02_cleaning.ipynb`) must focus on stripping out the 1,400+ mostly empty columns, isolating the 60 key indicators across 7 domains (including Gender & Resource Control), handling missing data logically, and standardizing units (e.g., converting local land size units to standard Hectares). No KPIs will be computed in cleaning — those belong in analysis notebooks 03/04.
