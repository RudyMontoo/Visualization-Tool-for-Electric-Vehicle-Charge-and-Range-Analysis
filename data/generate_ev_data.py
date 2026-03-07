import pandas as pd
import sqlite3
import os

def load_datasets_to_db():
    """Loads the real EV datasets from the dataset/ folder into SQLite."""
    
    db_path = os.path.join('data', 'ev_analytics.db')
    os.makedirs('data', exist_ok=True)

    print("Connecting to SQLite database...")
    conn = sqlite3.connect(db_path)
    
    # ------------------------------------------------------------------
    # Dataset 1: Cheapestelectriccars-EVDatabase.csv
    # Global EV reference — Used for Scenario 3 (Comparative Efficiency)
    # ------------------------------------------------------------------
    print("Loading: Cheapestelectriccars-EVDatabase.csv")
    df_ev_global = pd.read_csv('dataset/Cheapestelectriccars-EVDatabase.csv')
    
    # Normalize column names (strip whitespace)
    df_ev_global.columns = df_ev_global.columns.str.strip()
    print(f"  → {len(df_ev_global)} records | Columns: {list(df_ev_global.columns)}")
    
    df_ev_global.to_sql('ev_global_database', conn, if_exists='replace', index=False)
    df_ev_global.to_csv('data/ev_global_database.csv', index=False)

    # ------------------------------------------------------------------
    # Dataset 2: EVIndia (1).csv
    # Indian EV models — Used for Scenario 3 (India-specific comparison)
    # ------------------------------------------------------------------
    print("Loading: EVIndia (1).csv")
    df_ev_india = pd.read_csv('dataset/EVIndia (1).csv')
    df_ev_india.columns = df_ev_india.columns.str.strip()
    print(f"  → {len(df_ev_india)} records | Columns: {list(df_ev_india.columns)}")
    
    df_ev_india.to_sql('ev_india_models', conn, if_exists='replace', index=False)
    df_ev_india.to_csv('data/ev_india_models.csv', index=False)

    # ------------------------------------------------------------------
    # Dataset 3: ElectricCarData_Clean.csv
    # Cleaned EV spec data — Battery Performance (Scenario 2)
    # ------------------------------------------------------------------
    print("Loading: ElectricCarData_Clean.csv")
    df_ev_clean = pd.read_csv('dataset/ElectricCarData_Clean.csv')
    df_ev_clean.columns = df_ev_clean.columns.str.strip()
    print(f"  → {len(df_ev_clean)} records | Columns: {list(df_ev_clean.columns)}")
    
    df_ev_clean.to_sql('ev_car_data_clean', conn, if_exists='replace', index=False)
    df_ev_clean.to_csv('data/ev_car_data_clean.csv', index=False)

    # ------------------------------------------------------------------
    # Dataset 4: electric_vehicle_charging_station_list.csv
    # EV Charging Stations — Scenario 1 (Charging Patterns)
    # ------------------------------------------------------------------
    print("Loading: electric_vehicle_charging_station_list.csv")
    df_stations = pd.read_csv('dataset/electric_vehicle_charging_station_list.csv')
    df_stations.columns = df_stations.columns.str.strip()
    print(f"  → {len(df_stations)} records | Columns: {list(df_stations.columns)}")
    
    df_stations.to_sql('charging_stations', conn, if_exists='replace', index=False)
    df_stations.to_csv('data/charging_stations.csv', index=False)

    conn.close()
    
    print("\n✅ All 4 datasets loaded into:", db_path)
    print("   Tables: ev_global_database, ev_india_models, ev_car_data_clean, charging_stations")
    print("   CSVs also saved to data/ folder for Tableau Desktop / Tableau Public connection")

if __name__ == '__main__':
    load_datasets_to_db()
