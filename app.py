from flask import Flask, render_template
import sqlite3
import pandas as pd
import os

app = Flask(__name__)

DB_PATH = os.path.join('data', 'ev_analytics.db')

def get_db_stats():
    """Returns row counts for each table in the DB."""
    conn = sqlite3.connect(DB_PATH)
    stats = {}
    tables = ['ev_global_database', 'ev_india_models', 'ev_car_data_clean', 'charging_stations']
    for t in tables:
        try:
            stats[t] = pd.read_sql(f"SELECT COUNT(*) as count FROM {t}", conn)['count'][0]
        except:
            stats[t] = 0
    conn.close()
    return stats

def query_table(table, limit=10):
    """Returns a pandas DataFrame from the given table."""
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f"SELECT * FROM {table} LIMIT {limit}", conn)
    conn.close()
    return df

@app.route('/')
def index():
    stats = get_db_stats()
    return render_template('index.html', stats=stats)

@app.route('/dashboard/charging')
def dashboard_charging():
    df = query_table('charging_stations', limit=10)
    preview = df.to_html(classes='data-table', index=False, border=0) if not df.empty else None
    return render_template('dashboard.html',
        title='Charging Pattern and Usage Analysis',
        badge='Scenario 1',
        badge_class='badge-blue',
        scenario='Aarav, a city energy planner, uses this to understand when and where people are charging their EVs. This dashboard visualizes charging station locations, charger types, and power output.',
        tableau_placeholder='Tableau Dashboard for Charging Patterns goes here.',
        data_source_name='electric_vehicle_charging_station_list.csv',
        table_name='charging_stations',
        preview=preview)

@app.route('/dashboard/battery')
def dashboard_battery():
    df = query_table('ev_car_data_clean', limit=10)
    preview = df.to_html(classes='data-table', index=False, border=0) if not df.empty else None
    return render_template('dashboard.html',
        title='Battery Performance vs. Driving Range',
        badge='Scenario 2',
        badge_class='badge-green',
        scenario='Meera, a fleet manager, tracks how battery specs, efficiency, and power train affect driving range. This dashboard compares predicted vs. actual range across different models.',
        tableau_placeholder='Tableau Dashboard for Battery Performance goes here.',
        data_source_name='ElectricCarData_Clean.csv',
        table_name='ev_car_data_clean',
        preview=preview)

@app.route('/dashboard/efficiency')
def dashboard_efficiency():
    df = query_table('ev_global_database', limit=10)
    preview = df.to_html(classes='data-table', index=False, border=0) if not df.empty else None
    return render_template('dashboard.html',
        title='Comparative EV Model Efficiency Dashboard',
        badge='Scenario 3',
        badge_class='badge-purple',
        scenario='Ravi, an eco-conscious consumer, compares EV models on price, range, and efficiency. This dashboard shows both global models and India-specific options with personalized insights.',
        tableau_placeholder='Tableau Dashboard for Comparative EV Efficiency goes here.',
        data_source_name='Cheapestelectriccars-EVDatabase.csv + EVIndia.csv',
        table_name='ev_global_database',
        preview=preview)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
