import pandas.io.sql as sqlio
import psycopg2
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from dagster import op, In
from bokeh.plotting import figure, show, output_file
from sqlalchemy import create_engine, event, text, exc
from sqlalchemy.engine.url import URL
from bokeh.models import ColumnDataSource, FactorRange
from bokeh.transform import factor_cmap
from bokeh.palettes import Category20
from bokeh.layouts import gridplot
import plotly.express as px
import yaml
import plotly.graph_objects as go


def load_config():
    with open('config.yaml', 'r') as file:
        return yaml.safe_load(file)

config = load_config()

# Connection string to PostgreSQL database
postgres_connection_string = config['connection']['postgres_connection_string']

@op(
    ins={"start": In(bool)}
)

@op
def visualize_time_of_day_impact(start: bool):
    query_string = """
    SELECT COALESCE(crash_hour, -1) AS crash_hour, COUNT(*) AS num_crashes
    FROM traffic_incidents_table
    GROUP BY COALESCE(crash_hour, -1)
    ORDER BY COALESCE(crash_hour, -1);
    """
    engine = create_engine(postgres_connection_string)
    try:
        with engine.connect() as connection:
            df = sqlio.read_sql_query(query_string, connection)

        fig = px.bar(df, x='crash_hour', y='num_crashes', title='Impact of Time of Day on Crash Outcomes', 
                     labels={'crash_hour': 'Hour of the Day', 'num_crashes': 'Number of Crashes'})
        fig.show()

    finally:
        engine.dispose()


@op
def visualize_age_gender_impact(start: bool):
    # SQL query to fetch data
    query_string = """
    SELECT COALESCE("AGE", -1) AS "AGE", "SEX", "INJURY_CLASSIFICATION", COUNT(*) as cases
    FROM traffic_incidents_table
    WHERE "INJURY_CLASSIFICATION" IS NOT NULL 
    AND "SEX" IN ('M', 'F')
    AND "AGE" != -1
    GROUP BY COALESCE("AGE", -1), "SEX", "INJURY_CLASSIFICATION"
    ORDER BY COALESCE("AGE", -1), "SEX";
    """
    engine = create_engine(postgres_connection_string)
    try:
        with engine.connect() as connection:
            df = pd.read_sql(query_string, connection)
    
        df['AGE'] = df['AGE'].astype(str)
        output_file("age_gender_impact.html")

        # Filter DataFrame for 'M' and 'F' genders
        df_m = df[(df['SEX'] == 'M')]
        df_f = df[(df['SEX'] == 'F')]

        # Create separate plots for each gender
        create_plot(df_m, 'M', 'blue')
        create_plot(df_f, 'F', 'pink')
    finally:
        engine.dispose()

def create_plot(df_gender, gender, color):
    # Create a new plot with a title and axis labels
    p = figure(title=f"Impact of Age and Gender on Injury Severity for Gender {gender}", 
               x_axis_label='Age', 
               y_axis_label='Cases', 
               height=400, 
               width=950,
               x_range=FactorRange(factors=df_gender['AGE'].unique()))

    # Create a color mapper from the SEX column
    color_mapper = factor_cmap('SEX', palette=[color], factors=df_gender['SEX'].unique())

    # Create a ColumnDataSource from the DataFrame
    source = ColumnDataSource(df_gender)
    
    # Add vbar renderer with a legend and tooltip. Hover tool can be added if needed
    p.vbar(x='AGE', top='cases', width=0.9, source=source, color=color_mapper, legend_field="SEX")

    # Customize the legend location
    p.legend.title = 'Gender'
    p.legend.location = "top_right"

    # Rotate x-axis labels to prevent them from overlapping
    p.xaxis.major_label_orientation = "vertical"

    # Set the background color and transparency
    p.background_fill_color = "beige"
    p.background_fill_alpha = 0.5
 
    show(p)

@op
def visualize_safety_measures_effectiveness(start: bool):
    query_string = """
    SELECT "SAFETY_EQUIPMENT", "AIRBAG_DEPLOYED", "INJURY_CLASSIFICATION", COUNT(*) as cases
    FROM traffic_incidents_table
    WHERE "INJURY_CLASSIFICATION" IS NOT NULL
    GROUP BY "SAFETY_EQUIPMENT", "AIRBAG_DEPLOYED", "INJURY_CLASSIFICATION"
    ORDER BY "SAFETY_EQUIPMENT", "AIRBAG_DEPLOYED";
    """
    engine = create_engine(postgres_connection_string)
    with engine.connect() as connection:
        df = sqlio.read_sql_query(query_string, connection)

    # Plotting using Plotly
    fig = px.bar(df, x='SAFETY_EQUIPMENT', y='cases', color='AIRBAG_DEPLOYED',
                 title='Effectiveness of Safety Measures on Injury Severity',
                 labels={'SAFETY_EQUIPMENT': 'Safety Equipment', 'cases': 'Cases', 'AIRBAG_DEPLOYED': 'Airbag Deployed'})
    fig.show()
    engine.dispose()

@op
def visualize_environmental_impact(start: bool):
    query_string = """
    SELECT weather_condition, lighting_condition, COUNT(*) AS num_crashes
    FROM traffic_incidents_table
    GROUP BY weather_condition, lighting_condition
    ORDER BY weather_condition, lighting_condition;
    """
    engine = create_engine(postgres_connection_string)
    try:
        with engine.connect() as connection:
            df = sqlio.read_sql_query(query_string, connection)

        fig = px.bar(df, x='weather_condition', y='num_crashes', color='lighting_condition', 
                     title='Impact of Weather and Lighting Conditions on Crash Frequency', 
                     labels={'weather_condition': 'Weather Condition', 'num_crashes': 'Number of Crashes', 
                             'lighting_condition': 'Lighting Condition'})
        fig.show()

    finally:
        engine.dispose()

@op
def visualize_crash_causes(start: bool):
    engine = create_engine(postgres_connection_string)
    try:
        query = """
        SELECT prim_contributory_cause, "DRIVER_ACTION", "PHYSICAL_CONDITION", COUNT(*) AS num_cases
        FROM traffic_incidents_table
        GROUP BY prim_contributory_cause, "DRIVER_ACTION", "PHYSICAL_CONDITION"
        ORDER BY prim_contributory_cause, "DRIVER_ACTION", "PHYSICAL_CONDITION";
        """
        with engine.connect() as connection:
            df = pd.read_sql_query(query, engine)                
            cause_counts = df['prim_contributory_cause'].value_counts()

        fig1 = px.bar(y=cause_counts.index, x=cause_counts.values, orientation='h', 
                      title='Frequency of Primary Contributory Causes', 
                      labels={'x': 'Number of Cases', 'y': 'Primary Contributory Cause'})

        pivot_table = df.pivot_table(index='DRIVER_ACTION', columns='PHYSICAL_CONDITION', 
                                     values='num_cases', aggfunc='sum', fill_value=0)
        fig2 = go.Figure(data=go.Heatmap(z=pivot_table.values, x=pivot_table.columns, y=pivot_table.index, 
                                          colorscale='viridis'))
        fig2.update_layout(title='Driver Actions and Physical Conditions by Primary Cause', 
                           xaxis_title='Physical Condition', yaxis_title='Driver Action')

        fig1.show()
        fig2.show()

    finally:
       engine.dispose()

@op
def visualize_cell_phone_impact(start: bool):
    engine = create_engine(postgres_connection_string)
    try:
        query = """
        SELECT "CELL_PHONE_USE", COUNT(*) AS num_cases
        FROM traffic_incidents_table
        GROUP BY "CELL_PHONE_USE"
        ORDER BY "CELL_PHONE_USE";
        """
        df = pd.read_sql_query(query, engine)
        df['CELL_PHONE_USE'] = df['CELL_PHONE_USE'].fillna('Unknown')

        fig = px.bar(df, x='CELL_PHONE_USE', y='num_cases', 
                     title='Impact of Cell Phone Use on Crash Involvement', 
                     labels={'CELL_PHONE_USE': 'Cell Phone Use', 'num_cases': 'Number of Cases'})
        fig.show()

    finally:
        engine.dispose()

@op
def visualize_geographic_patterns(start: bool):
    query_string = """
    SELECT latitude, longitude, location
    FROM traffic_incidents_table;
    """
    engine = create_engine(postgres_connection_string)
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(query_string, connection)
        
        if df.empty:
            print("No data found.")
            return

        fig = px.scatter_geo(df, lat='latitude', lon='longitude', hover_name='location', 
                             title='Geographic Patterns of Traffic Incidents', 
                             projection='natural earth')
        fig.show()

    finally:
        engine.dispose()