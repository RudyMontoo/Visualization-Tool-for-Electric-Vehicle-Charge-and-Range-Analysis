"""
EV Analytics Hub - Flask Application
Covers: Web Integration, Data Filters, Visualization API
"""
from flask import Flask, render_template, jsonify, request
import sqlite3
import pandas as pd
import os

app = Flask(__name__)
DB_PATH = os.path.join('data', 'ev_analytics.db')

# ──────────────────────────────────────────────
# DB Helper
# ──────────────────────────────────────────────
def query(sql, params=None):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(sql, conn, params=params)
    conn.close()
    return df

def query_table(table, cols=None, limit=10):
    """Returns a pandas DataFrame from the given table, optionally selecting specific columns."""
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    col_clause = ', '.join(f'"{c}"' for c in cols) if cols else '*'
    df = pd.read_sql(f"SELECT {col_clause} FROM {table} LIMIT {limit}", conn)
    conn.close()
    return df

def get_db_stats():
    tables = {
        'charging_stations': 'Charging Stations',
        'ev_global_database': 'Global EV Models',
        'ev_car_data_clean': 'Battery Records',
        'ev_india_models': 'India EV Models'
    }
    conn = sqlite3.connect(DB_PATH)
    stats = {}
    for t, label in tables.items():
        try:
            stats[t] = pd.read_sql(f"SELECT COUNT(*) as c FROM {t}", conn)['c'][0]
        except:
            stats[t] = 0
    conn.close()
    return stats

# ──────────────────────────────────────────────
# HTML Routes
# ──────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html', stats=get_db_stats())


@app.route('/dashboard/charging')
def dashboard_charging():
    # Filter options
    sel_region = request.args.get('region', 'All')
    sel_type   = request.args.get('type', 'All')

    df = query("SELECT * FROM charging_stations_prepared")
    regions = ['All'] + sorted(df['region'].dropna().unique().tolist())
    types   = ['All'] + sorted(df['Charger_Class'].dropna().unique().tolist())

    if sel_region != 'All':
        df = df[df['region'] == sel_region]
    if sel_type != 'All':
        df = df[df['Charger_Class'] == sel_type]

    PREVIEW_COLS = ['region', 'type', 'power', 'service', 'Charger_Class', 'Power_kW', 'Est_Charge_Time_60kWh_hr']
    preview = df[PREVIEW_COLS].head(10).to_html(classes='data-table', index=False, border=0)
    return render_template('dashboard_charging.html',
        title='Charging Pattern and Usage Analysis',
        badge='Scenario 1', badge_class='badge-blue',
        scenario='Aarav, a city energy planner, analyzes EV charging station data — locations, charger types, and power outputs — to manage urban electricity demand.',
        preview=preview,
        regions=regions, sel_region=sel_region,
        types=types, sel_type=sel_type,
        total=len(df))


@app.route('/dashboard/battery')
def dashboard_battery():
    sel_powertrain = request.args.get('powertrain', 'All')
    sel_segment    = request.args.get('segment', 'All')

    df = query("SELECT * FROM ev_clean_prepared")
    powertrains = ['All'] + sorted(df['PowerTrain'].dropna().unique().tolist())
    segments    = ['All'] + sorted(df['Price_Segment'].dropna().unique().tolist())

    if sel_powertrain != 'All':
        df = df[df['PowerTrain'] == sel_powertrain]
    if sel_segment != 'All':
        df = df[df['Price_Segment'] == sel_segment]

    PREVIEW_COLS = ['Brand', 'Model', 'PowerTrain', 'Range_Km', 'Efficiency_WhKm', 'Cost_per_100km_EUR', 'Performance_Score', 'Price_Segment']
    preview = df[[c for c in PREVIEW_COLS if c in df.columns]].head(10).to_html(classes='data-table', index=False, border=0)
    return render_template('dashboard_battery.html',
        title='Battery Performance vs. Driving Range',
        badge='Scenario 2', badge_class='badge-green',
        scenario='Meera, a fleet manager, tracks how battery specs, powertrain type, and efficiency affect real-world driving range.',
        preview=preview,
        powertrains=powertrains, sel_powertrain=sel_powertrain,
        segments=segments, sel_segment=sel_segment,
        total=len(df))


@app.route('/dashboard/efficiency')
def dashboard_efficiency():
    sel_body  = request.args.get('body', 'All')
    sel_drive = request.args.get('drive', 'All')

    df = query("SELECT * FROM ev_global_prepared")
    bodies  = ['All'] + sorted(df['Drive'].dropna().unique().tolist())
    ratings = ['All'] + sorted(df['Efficiency_Rating'].dropna().unique().tolist())

    if sel_body != 'All':
        df = df[df['Drive'] == sel_body]
    if sel_drive != 'All':
        df = df[df['Efficiency_Rating'] == sel_drive]

    PREVIEW_COLS = ['Name', 'Drive', 'Range_km', 'Efficiency_Wh_km', 'Cost_per_100km_EUR', 'Efficiency_Rating', 'Range_Category', 'Annual_Savings_EUR']
    preview = df[[c for c in PREVIEW_COLS if c in df.columns]].head(10).to_html(classes='data-table', index=False, border=0)
    return render_template('dashboard_efficiency.html',
        title='Comparative EV Model Efficiency Dashboard',
        badge='Scenario 3', badge_class='badge-purple',
        scenario='Ravi, an eco-conscious consumer, compares EV models on price, range, efficiency, and annual savings to make a sustainable purchase.',
        preview=preview,
        bodies=bodies, sel_body=sel_body,
        ratings=ratings, sel_drive=sel_drive,
        total=len(df))


@app.route('/performance')
def performance():
    return render_template('performance.html')


# ──────────────────────────────────────────────
# JSON API Routes (for Chart.js)
# ──────────────────────────────────────────────

@app.route('/api/charging/by-region')
def api_charging_by_region():
    df = query("SELECT region, COUNT(*) as count FROM charging_stations_prepared GROUP BY region ORDER BY count DESC LIMIT 10")
    return jsonify({'labels': df['region'].tolist(), 'values': df['count'].tolist()})

@app.route('/api/charging/by-type')
def api_charging_by_type():
    df = query("SELECT Charger_Class, COUNT(*) as count FROM charging_stations_prepared GROUP BY Charger_Class")
    return jsonify({'labels': df['Charger_Class'].tolist(), 'values': df['count'].tolist()})

@app.route('/api/charging/by-power')
def api_charging_by_power():
    df = query("SELECT power, COUNT(*) as count FROM charging_stations_prepared GROUP BY power ORDER BY count DESC LIMIT 8")
    return jsonify({'labels': df['power'].tolist(), 'values': df['count'].tolist()})


@app.route('/api/battery/range-vs-efficiency')
def api_battery_range_vs_efficiency():
    df = query("SELECT Brand, Model, Range_Km, Efficiency_WhKm, PowerTrain FROM ev_clean_prepared WHERE Range_Km IS NOT NULL AND Efficiency_WhKm IS NOT NULL LIMIT 40")
    return jsonify({
        'datasets': [{
            'label': row['PowerTrain'],
            'x': row['Efficiency_WhKm'],
            'y': row['Range_Km'],
            'model': f"{row['Brand']} {row['Model']}"
        } for _, row in df.iterrows()]
    })

@app.route('/api/battery/top-range')
def api_battery_top_range():
    df = query("SELECT Brand || ' ' || Model as name, Range_Km FROM ev_clean_prepared WHERE Range_Km IS NOT NULL ORDER BY Range_Km DESC LIMIT 10")
    return jsonify({'labels': df['name'].tolist(), 'values': df['Range_Km'].tolist()})

@app.route('/api/battery/price-segment')
def api_battery_price_segment():
    df = query("SELECT Price_Segment, COUNT(*) as count FROM ev_clean_prepared WHERE Price_Segment IS NOT NULL GROUP BY Price_Segment")
    return jsonify({'labels': df['Price_Segment'].tolist(), 'values': df['count'].tolist()})


@app.route('/api/efficiency/top-range')
def api_efficiency_top_range():
    df = query("SELECT Name, Range_km FROM ev_global_prepared WHERE Range_km IS NOT NULL ORDER BY Range_km DESC LIMIT 10")
    return jsonify({'labels': df['Name'].tolist(), 'values': df['Range_km'].tolist()})

@app.route('/api/efficiency/cost-comparison')
def api_efficiency_cost_comparison():
    df = query("SELECT Name, Cost_per_100km_EUR FROM ev_global_prepared WHERE Cost_per_100km_EUR IS NOT NULL ORDER BY Cost_per_100km_EUR ASC LIMIT 10")
    return jsonify({'labels': df['Name'].tolist(), 'values': df['Cost_per_100km_EUR'].tolist()})

@app.route('/api/efficiency/range-category')
def api_efficiency_range_category():
    df = query("SELECT Range_Category, COUNT(*) as count FROM ev_global_prepared WHERE Range_Category IS NOT NULL GROUP BY Range_Category")
    return jsonify({'labels': df['Range_Category'].tolist(), 'values': df['count'].tolist()})

@app.route('/api/efficiency/drive-distribution')
def api_efficiency_drive_distribution():
    df = query("SELECT Drive, COUNT(*) as count FROM ev_global_prepared WHERE Drive IS NOT NULL GROUP BY Drive")
    return jsonify({'labels': df['Drive'].tolist(), 'values': df['count'].tolist()})


@app.route('/api/performance/stats')
def api_performance_stats():
    conn = sqlite3.connect(DB_PATH)
    tables = ['charging_stations', 'ev_global_database', 'ev_car_data_clean', 'ev_india_models',
              'charging_stations_prepared', 'ev_global_prepared', 'ev_clean_prepared', 'ev_india_prepared']
    results = []
    for t in tables:
        try:
            count = pd.read_sql(f"SELECT COUNT(*) as c FROM {t}", conn)['c'][0]
            cols  = pd.read_sql(f"SELECT * FROM {t} LIMIT 1", conn).shape[1]
            results.append({'table': t, 'rows': int(count), 'columns': int(cols)})
        except:
            pass
    conn.close()
    db_size_kb = round(os.path.getsize(DB_PATH) / 1024, 1)
    return jsonify({'tables': results, 'db_size_kb': db_size_kb, 'calc_fields': 17, 'visualizations': 8})


if __name__ == '__main__':
    app.run(debug=True, port=5000)
