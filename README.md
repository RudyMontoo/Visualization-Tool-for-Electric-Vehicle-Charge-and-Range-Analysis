# Electric Vehicle Charge and Range Analysis Visualization Tool

This repository contains  the backend and Flask frontend for an Interactive Tableau-based Visualization Tool analyzing Electric Vehicle (EV) usage.

## Setup Instructions

1. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Generate the Dataset & SQLite Database:**
   ```bash
   python data/generate_ev_data.py
   ```
   *This will generate `ev_analytics.db` and CSV files inside the `data/` folder.*

3. **Run the Flask Web App:**
   ```bash
   python app.py
   ```
   *Visit `http://127.0.0.1:5000` in your web browser.*

## Phase 2: Connecting Tableau (Tableau Team)

Now that the data is generated, you need to connect Tableau to the database.
1. Open Tableau Desktop.
2. Under "Connect To a File", select **More...** or "SQLite" (if you have the SQLite ODBC driver installed), otherwise use the generated CSV files in the `data/` folder.
3. Build the dashboards for the 3 Scenarios.
4. Publish the dashboards to Tableau Public.
5. In Tableau Public, click the 'Share' icon and copy the "Embed Code".
6. Navigate to `templates/dashboard.html` in this project and paste the embed code where it says `<!-- PASTE TABLEAU EMBED CODE HERE -->`.

## Team Assignments & Workflow

Below is the detailed list of Epics and Tasks mapped to our team members:

**Data Collection & Extraction from Database**
- **Collect the dataset:** Riya Verma
- **Storing Data in DB & Perform SQL Operations:** Rudra Pratapsingh
- **Connect DB with Tableau:** Rudra Sharma

**Data Preparation**
- **Prepare the Data for Visualization:** Saatyak Srivastav

**Data Visualization**
- **No of Unique Visualizations:** Rudra Sharma

**Dashboard**
- **Responsive and Design of Dashboard:** Rudra Sharma

**Story**
- **No of Scenes of Story:** Saatyak Srivastav

**Performance Testing** *(Unassigned)*
- Amount of Data Rendered to DB
- Utilization of Data Filters
- No of Calculation Fields
- No of Visualizations/ Graphs

**Web integration** *(Unassigned)*
- Publishing
- Dashboard and Story embed with UI With Flask

**Project Demonstration & Documentation** *(Unassigned)*
- Record explanation Video for project end to end solution
- Project Documentation-Step by step project development procedure
# Visualization-Tool-for-Electric-Vehicle-Charge-and-Range-Analysis
