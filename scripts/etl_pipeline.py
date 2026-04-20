import pandas as pd
import numpy as np
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class RhoMisETL:
    def __init__(self, raw_path, processed_path):
        self.raw_path = raw_path
        self.processed_path = processed_path
        self.df = None
        
        self.columns_to_keep = [
            'id_unique', 'country', 'region', 'year', 'count_people', 'respondentsex', 'age_malehead', 'age_femalehead',
            'landcultivated', 'landowned', 'unitland', 'land_tenure', 'local_currency',
            'crop_count', 'crop_name_1', 'crop_yield_1', 'crop_harvest_1',
            'crop_income_per_year_1', 'livestock_sale_income_1', 'offfarm_incomes_any',
            'hfias_1', 'hfias_2', 'hfias_3', 'hfias_4', 'hfias_5', 'hfias_6', 'hfias_7', 'hfias_8', 'hfias_9',
            'crop_consumed_prop_1'
        ]

    def extract(self):
        logging.info(f"Loading raw data from {self.raw_path}...")
        self.df = pd.read_csv(self.raw_path, low_memory=False)
        logging.info(f"Raw shape: {self.df.shape}")
        
    def _standardize_hectares(self, row):
        val = row.get('landcultivated', np.nan)
        unit = str(row.get('unitland', '')).lower()
        if pd.isna(val) or val == '': return np.nan
        try:
            val = float(val)
        except ValueError:
            return np.nan
        
        if 'acre' in unit:
            return val * 0.404686
        return val # Standardize assuming local mapping defaulted to HA

    def _fix_age(self, age):
        if pd.isna(age): return np.nan
        try:
            age = float(age)
        except:
            return np.nan
        # If they entered a birth year like 1978
        if age > 1900 and age < 2025:
            return 2024 - age
        # If age is completely impossible or a typo
        if age > 120 or age <= 0:
            return np.nan
        return age
            
    def transform(self):
        logging.info("Starting transformation phase...")
        df_clean = self.df[self.columns_to_keep].copy()
        
        # 0. Clean Demographics
        logging.info("Cleaning demographics (Sex & Age formats)...")
        # Standardize sex
        sex_map = {'m': 'Male', 'male': 'Male', 'f': 'Female', 'female': 'Female', 'both': 'Both', 'na': np.nan}
        df_clean['respondentsex'] = df_clean['respondentsex'].str.lower().str.strip().map(sex_map).fillna(np.nan)
        
        # Fix Ages (turn birth years into ages)
        df_clean['age_malehead'] = df_clean['age_malehead'].apply(self._fix_age)
        df_clean['age_femalehead'] = df_clean['age_femalehead'].apply(self._fix_age)
        
        # Ensure count_people is numeric & minimum 1
        df_clean['count_people'] = pd.to_numeric(df_clean['count_people'], errors='coerce')
        df_clean['count_people'] = df_clean['count_people'].fillna(df_clean['count_people'].median())
        df_clean['count_people'] = df_clean['count_people'].clip(lower=1) # Prevent divide by zero
        
        # 1. Land standardization & Outliers
        logging.info("Standardizing land measurements...")
        df_clean['land_cultivated_ha'] = df_clean.apply(self._standardize_hectares, axis=1)
        # Cap impossible land sizes at the 99th percentile (~1500 ha) to avoid visual skew in Tableau
        df_clean['land_cultivated_ha'] = df_clean['land_cultivated_ha'].clip(upper=1500)
        
        df_clean['farm_size_bucket'] = pd.cut(
            df_clean['land_cultivated_ha'],
            bins=[-np.inf, 2, 10, np.inf], 
            labels=['Smallholder (<=2ha)', 'Medium (2-10ha)', 'Large (>10ha)']
        )

        # 2. Income engineering
        logging.info("Engineering Income metrics...")
        for col in ['crop_income_per_year_1', 'livestock_sale_income_1']:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0)
            
        df_clean['total_annual_income'] = df_clean['crop_income_per_year_1'] + df_clean['livestock_sale_income_1']
        df_clean['income_per_capita'] = df_clean['total_annual_income'] / df_clean['count_people']

        # 3. HFIAS Scaling
        logging.info("Scoring food security indicators...")
        hfias_map = {
            'never': 0, 
            'fewpermonth': 1, 'monthly': 1, 
            'fewperweek': 2, 'weekly': 2, 
            'daily': 3
        }
        
        hfias_cols = [f'hfias_{i}' for i in range(1, 10)]
        score_cols = []
        for c in hfias_cols:
            score_col = f"{c}_score"
            df_clean[score_col] = df_clean[c].str.lower().map(hfias_map)
            score_cols.append(score_col)
            
        df_clean['hfias_total_score'] = df_clean[score_cols].sum(axis=1, min_count=1)
        
        def map_food_security(val):
            if pd.isna(val): return np.nan
            if val <= 5: return 'Food Secure'
            if val <= 10: return 'Mildly Insecure'
            if val <= 17: return 'Moderately Insecure'
            return 'Severely Insecure'
            
        df_clean['food_security_status'] = df_clean['hfias_total_score'].apply(map_food_security)

        # Drop interim calculation columns
        self.df = df_clean.drop(columns=score_cols + hfias_cols + ['unitland', 'landcultivated'])
        logging.info(f"Transformation complete. Clean shape: {self.df.shape}")

    def load(self):
        os.makedirs(os.path.dirname(self.processed_path), exist_ok=True)
        self.df.to_csv(self.processed_path, index=False)
        logging.info(f"Successfully saved clean dataset to {self.processed_path}")

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    raw_csv = os.path.join(current_dir, "../data/raw/raw.csv.gz")
    processed_csv = os.path.join(current_dir, "../data/processed/cleaned_rhomis_data.csv")
    
    etl = RhoMisETL(raw_path=raw_csv, processed_path=processed_csv)
    etl.extract()
    etl.transform()
    etl.load()
