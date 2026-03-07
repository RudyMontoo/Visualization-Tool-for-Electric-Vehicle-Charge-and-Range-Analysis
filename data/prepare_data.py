"""
Data Preparation Script for EV Analytics
==========================================
Epic: Data Preparation — Prepare the Data for Visualization
Epic: Performance Testing — No of Calculation Fields

This script:
1. Cleans all raw datasets
2. Adds calculated/derived fields
3. Saves enriched tables back to ev_analytics.db
4. Reports calculated field count and data quality stats
"""

import pandas as pd
import sqlite3
import os
import time

DB_PATH = os.path.join('data', 'ev_analytics.db')

def connect():
    return sqlite3.connect(DB_PATH)

# ──────────────────────────────────────────────
# 1. PREPARE: EV Global Database (Scenario 3)
# ──────────────────────────────────────────────
def prepare_ev_global(conn):
    print("\n[1] Preparing ev_global_database...")
    df = pd.read_sql("SELECT * FROM ev_global_database", conn)

    # Clean: strip whitespace from all string columns
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Clean: parse numeric ranges (e.g. "335 km" → 335)
    def extract_num(val):
        if isinstance(val, str):
            cleaned = ''.join(c for c in val if c.isdigit() or c == '.')
            return float(cleaned) if cleaned else None
        return val

    df['Range_km']       = df['Range'].apply(extract_num)
    df['Efficiency_Wh_km'] = df['Efficiency'].apply(extract_num)
    df['TopSpeed_kmh']   = df['TopSpeed'].apply(extract_num)
    df['Accel_sec']      = df['Acceleration'].apply(extract_num)

    # CALCULATED FIELDS
    electricity_cost_eur_per_kwh = 0.30  # avg European rate
    df['Cost_per_100km_EUR'] = ((df['Efficiency_Wh_km'] / 1000) * 100 * electricity_cost_eur_per_kwh).round(2)            # CF1

    df['Efficiency_Rating'] = pd.cut(
        df['Efficiency_Wh_km'],
        bins=[0, 150, 200, 250, 999],
        labels=['Excellent', 'Good', 'Average', 'Poor']
    ).astype(str)                                                                                                           # CF2

    df['Range_Category'] = pd.cut(
        df['Range_km'],
        bins=[0, 200, 300, 400, 9999],
        labels=['Short (<200km)', 'Medium (200-300km)', 'Long (300-400km)', 'Ultra-Long (>400km)']
    ).astype(str)                                                                                                           # CF3

    # Annual fuel savings vs petrol (12,000 km/yr, 8L/100km petrol @ €1.80/L)
    annual_petrol_cost = (12000 / 100) * 8 * 1.80
    annual_ev_cost = (df['Efficiency_Wh_km'] / 1000) * 12000 * electricity_cost_eur_per_kwh
    df['Annual_Savings_EUR'] = (annual_petrol_cost - annual_ev_cost).round(2)                                              # CF4

    df['Value_Score'] = (df['Range_km'] / df['Efficiency_Wh_km'] * 10).round(2)                                           # CF5

    df.to_sql('ev_global_prepared', conn, if_exists='replace', index=False)
    df.to_csv('data/ev_global_prepared.csv', index=False)
    print(f"   ✓ {len(df)} rows | Calculated fields added: Cost_per_100km, Efficiency_Rating, Range_Category, Annual_Savings, Value_Score")
    return df, 5  # returns (df, num_calculated_fields)

# ──────────────────────────────────────────────
# 2. PREPARE: EV Car Data Clean (Scenario 2)
# ──────────────────────────────────────────────
def prepare_ev_clean(conn):
    print("\n[2] Preparing ev_car_data_clean...")
    df = pd.read_sql("SELECT * FROM ev_car_data_clean", conn)
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Convert key columns to numeric (handle '-' or missing values)
    df['FastCharge_KmH'] = pd.to_numeric(df['FastCharge_KmH'], errors='coerce')
    df['Range_Km']       = pd.to_numeric(df['Range_Km'], errors='coerce')
    df['Efficiency_WhKm'] = pd.to_numeric(df['Efficiency_WhKm'], errors='coerce')
    df['AccelSec']       = pd.to_numeric(df['AccelSec'], errors='coerce')
    df['TopSpeed_KmH']   = pd.to_numeric(df['TopSpeed_KmH'], errors='coerce')
    df['PriceEuro']      = pd.to_numeric(df['PriceEuro'], errors='coerce')

    # CALCULATED FIELDS
    electricity_cost = 0.30
    df['Cost_per_100km_EUR'] = ((df['Efficiency_WhKm'] / 1000) * 100 * electricity_cost).round(2)                         # CF6

    df['FastCharge_Time_to_80pct_min'] = (0.8 * df['Range_Km'] / df['FastCharge_KmH'] * 60).round(1)                     # CF7

    df['Performance_Score'] = (
        (1 / df['AccelSec'].replace(0, 1)) * 100 +          # faster = better
        df['TopSpeed_KmH'] / 10
    ).round(2)                                                                                                              # CF8

    df['Range_per_kWh_price'] = (df['Range_Km'] / (df['PriceEuro'] / 1000)).round(2)                                     # CF9: km of range per euro spent (1000s)

    price_bins = [0, 35000, 50000, 70000, float('inf')]
    price_labels = ['Budget (<35K)', 'Mid-Range (35-50K)', 'Premium (50-70K)', 'Luxury (>70K)']
    df['Price_Segment'] = pd.cut(df['PriceEuro'], bins=price_bins, labels=price_labels).astype(str)                       # CF10

    df.to_sql('ev_clean_prepared', conn, if_exists='replace', index=False)
    df.to_csv('data/ev_clean_prepared.csv', index=False)
    print(f"   ✓ {len(df)} rows | Calculated fields: Cost_per_100km, FastCharge_Time, Performance_Score, Range_per_EurK, Price_Segment")
    return df, 5

# ──────────────────────────────────────────────
# 3. PREPARE: Charging Stations (Scenario 1)
# ──────────────────────────────────────────────
def prepare_charging_stations(conn):
    print("\n[3] Preparing charging_stations...")
    df = pd.read_sql("SELECT * FROM charging_stations", conn)
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # CALCULATED FIELDS
    def extract_power_kw(power_str):
        if isinstance(power_str, str):
            cleaned = ''.join(c for c in power_str if c.isdigit() or c == '.')
            return float(cleaned) if cleaned else None
        return power_str

    df['Power_kW'] = df['power'].apply(extract_power_kw)                                                                  # CF11

    def charger_class(t):
        if isinstance(t, str):
            if 'DC' in t.upper(): return 'DC Fast Charger'
            if 'AC' in t.upper(): return 'AC Charger'
        return 'Unknown'
    df['Charger_Class'] = df['type'].apply(charger_class)                                                                  # CF12

    # Estimated charge time to fill 60kWh battery (most common size)
    df['Est_Charge_Time_60kWh_hr'] = (60 / df['Power_kW']).round(2)                                                       # CF13

    df.to_sql('charging_stations_prepared', conn, if_exists='replace', index=False)
    df.to_csv('data/charging_stations_prepared.csv', index=False)
    print(f"   ✓ {len(df)} rows | Calculated fields: Power_kW, Charger_Class, Est_Charge_Time_60kWh")
    return df, 3

# ──────────────────────────────────────────────
# 4. PREPARE: India EV Models (Scenario 3 - India)
# ──────────────────────────────────────────────
def prepare_ev_india(conn):
    print("\n[4] Preparing ev_india_models...")
    df = pd.read_sql("SELECT * FROM ev_india_models", conn)
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    def extract_num(val):
        if isinstance(val, str):
            cleaned = ''.join(c for c in val if c.isdigit() or c == '.')
            return float(cleaned) if cleaned else None
        return val

    # Parse price range: "17.4" (Lakhs) → keep as float
    df['Price_Lakhs'] = df['PriceRange(Lakhs)'].apply(extract_num)                                                         # CF14
    df['Price_INR'] = (df['Price_Lakhs'] * 100000).round(0)                                                               # CF15

    # Range extraction (e.g. "312 Km/Full Charge" → 312)
    df['Range_km'] = df['Range'].apply(lambda x: extract_num(str(x).split('Km')[0]) if isinstance(x, str) else x)         # CF16

    # Cost per km (assuming ₹8/kWh avg Indian rate, ~5 km/kWh for EVs)
    electricity_inr_per_km = 8 / 5  # ₹1.6 per km
    df['Cost_per_km_INR'] = round(electricity_inr_per_km, 2)                                                              # CF17

    df.to_sql('ev_india_prepared', conn, if_exists='replace', index=False)
    df.to_csv('data/ev_india_prepared.csv', index=False)
    print(f"   ✓ {len(df)} rows | Calculated fields: Price_Lakhs, Price_INR, Range_km, Cost_per_km_INR")
    return df, 4

# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
if __name__ == '__main__':
    print("=" * 60)
    print("EV DATA PREPARATION — Calculated Fields Generator")
    print("=" * 60)

    conn = connect()
    start = time.time()

    _, cf1 = prepare_ev_global(conn)
    _, cf2 = prepare_ev_clean(conn)
    _, cf3 = prepare_charging_stations(conn)
    _, cf4 = prepare_ev_india(conn)

    elapsed = round(time.time() - start, 2)
    total_cf = cf1 + cf2 + cf3 + cf4

    conn.close()

    print("\n" + "=" * 60)
    print(f"✅ Data Preparation Complete in {elapsed}s")
    print(f"   Total Calculated Fields Created : {total_cf}")
    print(f"   Prepared tables saved to        : data/ev_analytics.db")
    print(f"   Prepared CSVs saved to          : data/*_prepared.csv")
    print("=" * 60)
