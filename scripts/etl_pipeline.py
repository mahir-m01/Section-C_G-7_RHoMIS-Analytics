#!/usr/bin/env python3
"""
ETL Pipeline — RHoMIS Analytics (Section C, Group 7)

Usage:  python scripts/etl_pipeline.py

Outputs:
  data/processed/rhomis_cleaned.csv
  data/processed/rhomis_cleaned.csv.gz
  data/processed/rhomis_cleaning_log.csv
  data/processed/rhomis_cleaning_summary.json
"""

import pandas as pd
import numpy as np
import json
import logging
from pathlib import Path
from datetime import datetime
from collections import Counter

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("etl")

_tlog: list = []
_step: int = 0

# Required columns from the cleaning contract
REQUIRED_COLS = [
    "id_unique","country","iso_country_code","year","region",
    "gps_lat_rounded","gps_lon_rounded",
    "respondentsex","age_malehead","age_femalehead","education_level",
    "count_people","children_under_4","children_4to10",
    "males11to24","females11to24","males25to50","females25to50",
    "malesover50","femalesover50","respondent_is_head",
    "landcultivated","unitland","land_tenure","land_irrigated","farm_labour",
    "crop_count",
    "crop_name_1","crop_name_2","crop_name_3","crop_name_4","crop_name_5",
    "crop_harvest_kg_per_year_1","crop_harvest_kg_per_year_2","crop_harvest_kg_per_year_3",
    "crop_income_per_year_1","crop_income_per_year_2","crop_income_per_year_3",
    "crop_land_area_1","crop_land_area_2","crop_land_area_3",
    "crop_consumed_prop_1","crop_consumed_prop_2","crop_consumed_prop_3",
    "local_currency","offfarm_incomes_any","offfarm_income_proportion",
    "livestock_sale_income_1","livestock_sale_income_2",
    "hfias_1","hfias_2","hfias_3","hfias_4","hfias_5",
    "hfias_6","hfias_7","hfias_8","hfias_9",
    "fies_1","fies_2","fies_3","fies_4","fies_5","fies_6","fies_7","fies_8",
    "foodshortagetime",
    "land_ownership",
    "crop_who_control_revenue_1","crop_who_control_revenue_2","crop_who_control_revenue_3",
    "offfarm_who_control_revenue_1","offfarm_who_control_revenue_2",
    "livestock_meat_who_control_eating_1","dairy_products_who_control_eating",
]

# Land-unit to hectare multiplier
UNIT_HA = {
    "hectare":1,"hectares":1,"ha":1,
    "acre":0.404686,"acres":0.404686,
    "timad":0.25,"timad_0.25ha":0.25,
    "are":0.01,"ares":0.01,"are_25x25m":0.01,"carre_25x25m":0.01,
    "m2":0.0001,"metre_metre":0.0001,
    "manzanas":0.7,"manzana":0.7,
    "dunum_1000m2":0.1,
    "sao":0.036,
    "bigha":0.677,
    "katha":0.034,"kattha":0.034,
    "nali":0.02,"naali":0.02,"nalies":0.02,
    "lima":0.09,
    "igito_60x60m":0.36,
    "kanal":0.05,
    "cuadra_7056msq":0.7056,
}

# ── Helper ───────────────────────────────────────────────────────────────

def log_step(name, col, action, rows, details=""):
    global _step
    _step += 1
    _tlog.append({"step_number":_step,"step_name":name,"column":col,
                  "action":action,"rows_affected":int(rows),"details":details})
    log.info(f"[{_step}] {name} | {col} | {action} | n={rows} | {details}")

# ── Extract ──────────────────────────────────────────────────────────────

def get_project_paths():
    root = Path(__file__).resolve().parent.parent
    raw_dir = root / "data" / "raw"
    proc = root / "data" / "processed"
    for name in ("full_survey_data.csv", "raw.csv.gz"):
        p = raw_dir / name
        if p.exists():
            return {"root":root,"raw_file":p,"proc":proc,
                    "csv":proc/"rhomis_cleaned.csv",
                    "gz":proc/"rhomis_cleaned.csv.gz",
                    "log_csv":proc/"rhomis_cleaning_log.csv",
                    "summary":proc/"rhomis_cleaning_summary.json"}
    raise FileNotFoundError(f"No raw data in {raw_dir}")


def load_raw_data(paths):
    """Read only the columns we need using usecols."""
    rf = paths["raw_file"]
    log.info(f"Reading {rf.name} …")

    # Read header to find which required columns exist
    header = pd.read_csv(rf, nrows=0).columns.tolist()
    present = [c for c in REQUIRED_COLS if c in header]
    missing = [c for c in REQUIRED_COLS if c not in header]

    if missing:
        log_step("extract","*","missing_expected_columns",0,f"missing={missing}")
        log.warning(f"Missing columns (will continue): {missing}")

    # Load only the columns that exist
    df = pd.read_csv(rf, usecols=present, low_memory=False)
    raw_shape = (df.shape[0], len(header))  # report full raw width

    log_step("extract","*","load_raw_file",df.shape[0],
             f"raw_file={rf.name}, raw_shape=({raw_shape[0]},{raw_shape[1]}), "
             f"loaded_cols={len(present)}")
    return df, raw_shape, missing


# ── Transform ────────────────────────────────────────────────────────────

def clean_strings(df):
    """Strip whitespace, lowercase, blank strings to NaN."""
    str_cols = df.select_dtypes(include=["object","string"]).columns.tolist()
    for col in str_cols:
        before_nan = df[col].isna().sum()
        s = df[col].copy()
        notna = s.notna()
        s[notna] = s[notna].astype(str).str.strip().str.lower()
        # blank strings to NaN
        s = s.replace({"": np.nan})
        df[col] = s
        changed = df[col].isna().sum() - before_nan
        if changed > 0:
            log_step("clean_strings",col,"strip_lower_blank_to_nan",
                     changed,f"new_NaN={changed}")
    log_step("clean_strings","*","general_string_pass",0,
             f"processed {len(str_cols)} string columns")
    return df


def clean_country(df):
    if "country" not in df.columns:
        return df
    fixes = {"philipines":"philippines","philipines ":"philippines"}
    changed = 0
    for old, new in fixes.items():
        mask = df["country"] == old
        changed += mask.sum()
        df.loc[mask,"country"] = new
    log_step("clean_country","country","fix_misspellings",changed,
             f"philipines→philippines ({changed} rows)")
    return df


def clean_currency(df):
    if "local_currency" not in df.columns:
        return df
    cfa_map = {"cfa_franc":"xof","fcfa_xof":"xof","fcfa":"xof","cfa":"xof"}
    changed = 0
    for old, new in cfa_map.items():
        mask = df["local_currency"] == old
        changed += mask.sum()
        df.loc[mask,"local_currency"] = new
    log_step("clean_currency","local_currency","standardise_cfa_to_xof",
             changed,f"CFA variants → xof ({changed} rows)")
    # Notebook maps '1' → NaN (garbage entry)
    g_mask = df["local_currency"] == "1"
    g_count = g_mask.sum()
    if g_count:
        df.loc[g_mask,"local_currency"] = np.nan
        log_step("clean_currency","local_currency","garbage_1_to_nan",
                 g_count,"from notebook logic")
    return df


def clean_ages(df):
    """Convert birth years using row-wise survey year, cap at 0–110."""
    for col in ("age_malehead","age_femalehead"):
        if col not in df.columns:
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")
        birth_converted = 0

        for idx in df.index:
            val = df.at[idx, col]
            yr  = df.at[idx, "year"] if "year" in df.columns else np.nan
            if pd.isna(val) or pd.isna(yr):
                continue
            if 1900 < val < yr:
                df.at[idx, col] = yr - val
                birth_converted += 1

        # Cap impossible ages
        bad = (df[col] < 0) | (df[col] > 110)
        impossible = bad.sum()
        df.loc[bad, col] = np.nan

        log_step("clean_ages",col,"birth_year_to_age",birth_converted,
                 f"converted={birth_converted}")
        log_step("clean_ages",col,"impossible_age_to_nan",impossible,
                 f"<0 or >110 set to NaN")
        log_step("clean_ages",col,"final_missing",df[col].isna().sum(),
                 f"missing after cleaning={df[col].isna().sum()}")
    return df


def derive_household_size(df):
    bands = ["children_under_4","children_4to10","males11to24","females11to24",
             "males25to50","females25to50","malesover50","femalesover50"]
    present = [c for c in bands if c in df.columns]
    for c in present:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["household_size_derived"] = df[present].sum(axis=1)
    log_step("derive_household_size","household_size_derived",
             "sum_age_bands",df.shape[0],
             f"summed {len(present)} bands; count_people left with NaN intact")
    return df


def convert_land_to_hectares(df):
    if "landcultivated" not in df.columns or "unitland" not in df.columns:
        log_step("convert_land","landcultivated","skipped",0,"column missing")
        return df
    df["landcultivated"] = pd.to_numeric(df["landcultivated"], errors="coerce")
    success = 0
    failed = 0
    results = pd.Series(np.nan, index=df.index)
    unknown_units = []

    for idx in df.index:
        val = df.at[idx,"landcultivated"]
        unit = df.at[idx,"unitland"]
        if pd.isna(val):
            continue
        if pd.isna(unit):
            failed += 1
            continue
        u = str(unit).strip().lower()
        if u in UNIT_HA:
            results.at[idx] = val * UNIT_HA[u]
            success += 1
        else:
            unknown_units.append(u)
            failed += 1

    df["land_cultivated_ha"] = results
    top_unknown = Counter(unknown_units).most_common(10)
    log_step("convert_land","land_cultivated_ha","unit_conversion",success,
             f"success={success}, failed={failed}")
    log_step("convert_land","land_cultivated_ha","unknown_units",failed,
             f"top_unknown={top_unknown}")
    log_step("convert_land","land_cultivated_ha","no_clip_applied",0,
             "notebook clips >1500ha but contract does not require it")
    return df


def clean_binary_columns(df):
    """Standardise binary yes/no fields and respondentsex."""
    bin_cols = ["offfarm_incomes_any","land_irrigated",
                "respondent_is_head","foodshortagetime"]
    yes_vals = {"y","yes","true","1"}
    no_vals  = {"n","no","false","0"}

    for col in bin_cols:
        if col not in df.columns:
            continue
        before = df[col].copy()
        def _map_bin(v):
            if pd.isna(v): return np.nan
            vl = str(v).strip().lower()
            if vl in yes_vals: return "y"
            if vl in no_vals:  return "n"
            return np.nan
        df[col] = df[col].apply(_map_bin)
        invalid = (before.notna() & df[col].isna()).sum()
        if invalid:
            log_step("clean_binary",col,"invalid_to_nan",invalid,
                     "cohead/dont_know/other → NaN")
        else:
            log_step("clean_binary",col,"standardised",0,"y/n mapping applied")

    # respondentsex: keep only m/f
    if "respondentsex" in df.columns:
        before = df["respondentsex"].copy()
        def _map_sex(v):
            if pd.isna(v): return np.nan
            vl = str(v).strip().lower()
            if vl in ("m","male"): return "m"
            if vl in ("f","female","woman"): return "f"
            return np.nan
        df["respondentsex"] = df["respondentsex"].apply(_map_sex)
        inv = (before.notna() & df["respondentsex"].isna()).sum()
        log_step("clean_respondentsex","respondentsex","keep_m_f_only",inv,
                 f"invalid → NaN ({inv} rows)")
    return df


def clean_food_security_columns(df):
    """Clean HFIAS and FIES string columns."""
    # HFIAS: keep no_answer as valid
    hfias_ok = {"never","monthly","fewpermonth","weekly","fewperweek","daily","no_answer"}
    for i in range(1,10):
        col = f"hfias_{i}"
        if col not in df.columns:
            continue
        before_valid = df[col].notna().sum()
        def _hf(v):
            if pd.isna(v): return np.nan
            vl = str(v).strip().lower()
            return vl if vl in hfias_ok else np.nan
        df[col] = df[col].apply(_hf)
        inv = before_valid - df[col].notna().sum()
        if inv:
            log_step("clean_hfias",col,"invalid_to_nan",inv,
                     f"invalid→NaN ({inv})")

    # FIES: yes→y, no→n, keep no_answer
    fies_ok = {"y","n","no_answer"}
    for i in range(1,9):
        col = f"fies_{i}"
        if col not in df.columns:
            continue
        before_valid = df[col].notna().sum()
        def _fi(v):
            if pd.isna(v): return np.nan
            vl = str(v).strip().lower()
            if vl in ("yes","y"): return "y"
            if vl in ("no","n"):  return "n"
            if vl == "no_answer": return "no_answer"
            return np.nan
        df[col] = df[col].apply(_fi)
        inv = before_valid - df[col].notna().sum()
        if inv:
            log_step("clean_fies",col,"invalid_to_nan",inv,f"{inv} invalid→NaN")
    return df


def clean_offfarm_income_proportion(df):
    """Clean offfarm_income_proportion as a categorical column."""
    col = "offfarm_income_proportion"
    if col not in df.columns:
        return df
    valid = {"little","underhalf","half","most","all","none"}
    bv = df[col].notna().sum()
    def _clean(v):
        if pd.isna(v): return np.nan
        vl = str(v).strip().lower()
        return vl if vl in valid else np.nan
    df[col] = df[col].apply(_clean)
    inv = bv - df[col].notna().sum()
    log_step("clean_offfarm_proportion",col,"invalid_to_nan",inv,
             f"kept valid categories; {inv} invalid→NaN")
    return df


def clean_crop_columns(df):
    """Clean crop land area, consumed proportion, and numeric columns."""
    # crop_land_area: categorical
    area_ok = {"little","underhalf","half","most","all","none"}
    for col in ("crop_land_area_1","crop_land_area_2","crop_land_area_3"):
        if col not in df.columns:
            continue
        bv = df[col].notna().sum()
        def _cla(v):
            if pd.isna(v): return np.nan
            vl = str(v).strip().lower()
            return vl if vl in area_ok else np.nan
        df[col] = df[col].apply(_cla)
        inv = bv - df[col].notna().sum()
        log_step("clean_crop_land_area",col,"invalid_to_nan",inv,
                 f"numeric/invalid removed ({inv})")

    # crop_consumed_prop: categorical
    prop_ok = {"none","little","underhalf","half","most","all"}
    for col in ("crop_consumed_prop_1","crop_consumed_prop_2","crop_consumed_prop_3"):
        if col not in df.columns:
            continue
        bv = df[col].notna().sum()
        def _ccp(v):
            if pd.isna(v): return np.nan
            vl = str(v).strip().lower()
            return vl if vl in prop_ok else np.nan
        df[col] = df[col].apply(_ccp)
        inv = bv - df[col].notna().sum()
        log_step("clean_crop_consumed_prop",col,"invalid_to_nan",inv,
                 f"invalid→NaN ({inv})")

    # Numeric coercion (no offfarm_income_proportion here)
    num_cols = [
        "crop_income_per_year_1","crop_income_per_year_2","crop_income_per_year_3",
        "crop_harvest_kg_per_year_1","crop_harvest_kg_per_year_2","crop_harvest_kg_per_year_3",
        "livestock_sale_income_1","livestock_sale_income_2",
        "crop_count","gps_lat_rounded","gps_lon_rounded","count_people",
    ]
    for col in num_cols:
        if col not in df.columns:
            continue
        bv = df[col].notna().sum()
        df[col] = pd.to_numeric(df[col], errors="coerce")
        inv = bv - df[col].notna().sum()
        if inv:
            log_step("numeric_coerce",col,"to_numeric_coerce",inv,
                     f"{inv} non-numeric→NaN")
    return df


def clean_gender_control_columns(df):
    """Strip/lowercase gender and resource-control columns."""
    gcols = [
        "land_ownership",
        "crop_who_control_revenue_1","crop_who_control_revenue_2",
        "crop_who_control_revenue_3",
        "offfarm_who_control_revenue_1","offfarm_who_control_revenue_2",
        "livestock_meat_who_control_eating_1","dairy_products_who_control_eating",
    ]
    for col in gcols:
        if col not in df.columns:
            log_step("clean_gender",col,"column_missing",0,"not in raw data")
            continue
        bv = df[col].notna().sum()
        s = df[col].copy()
        mask = s.notna()
        s[mask] = s[mask].astype(str).str.strip().str.lower()
        # no_answer → NaN only for gender columns
        s = s.replace({"no_answer":np.nan,"":np.nan})
        df[col] = s
        inv = bv - df[col].notna().sum()
        log_step("clean_gender",col,"strip_lower_noanswer",inv,
                 f"no_answer/blank→NaN ({inv})")
    return df

# ── Load ─────────────────────────────────────────────────────────────────

def save_outputs(df, paths, raw_shape, missing_cols):
    proc = paths["proc"]
    proc.mkdir(parents=True, exist_ok=True)

    df.to_csv(paths["csv"], index=False)
    log.info(f"Saved {paths['csv']}")

    df.to_csv(paths["gz"], index=False, compression="gzip")
    log.info(f"Saved {paths['gz']}")

    log_df = pd.DataFrame(_tlog)
    log_df.to_csv(paths["log_csv"], index=False)
    log.info(f"Saved {paths['log_csv']} ({len(_tlog)} entries)")

    summary = {
        "raw_input_path": str(paths["raw_file"]),
        "output_path": str(paths["csv"]),
        "run_timestamp": datetime.now().isoformat(),
        "raw_shape": {"rows":raw_shape[0],"cols":raw_shape[1]},
        "cleaned_shape": {"rows":df.shape[0],"cols":df.shape[1]},
        "number_of_selected_columns": len(REQUIRED_COLS),
        "missing_expected_columns": missing_cols,
        "output_columns": df.columns.tolist(),
        "total_transformations_logged": len(_tlog),
        "final_missing_values_per_column": df.isnull().sum().to_dict(),
        "duplicate_row_count": int(df.duplicated().sum()),
        "notes": [
            "No imputation performed except household_size_derived from age-band columns.",
            "No analysis KPIs created (no total_annual_income, income_per_capita, "
            "food_security_status, hfias_total_score, farm_size_bucket).",
            "count_people left with NaN intact.",
            "HFIAS and FIES exported as cleaned strings, not scored.",
            "offfarm_income_proportion treated as categorical, not numeric.",
        ],
    }
    with open(paths["summary"], "w") as f:
        json.dump(summary, f, indent=2, default=str)
    log.info(f"Saved {paths['summary']}")

# ── Main ─────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("RHoMIS ETL Pipeline — START")
    log.info("=" * 60)

    # Extract
    paths = get_project_paths()
    df, raw_shape, missing_cols = load_raw_data(paths)

    # Transform
    df = clean_strings(df)
    df = clean_country(df)
    df = clean_currency(df)
    df = clean_ages(df)
    df = derive_household_size(df)
    df = convert_land_to_hectares(df)
    df = clean_binary_columns(df)
    df = clean_food_security_columns(df)
    df = clean_offfarm_income_proportion(df)
    df = clean_crop_columns(df)
    df = clean_gender_control_columns(df)

    # Load
    save_outputs(df, paths, raw_shape, missing_cols)

    log.info("=" * 60)
    log.info(f"DONE — cleaned shape: {df.shape}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
