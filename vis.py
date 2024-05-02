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

def load_config():
    with open('config.yaml', 'r') as file:
        return yaml.safe_load(file)

config = load_config()

# Connection string to PostgreSQL database
postgres_connection_string = config['connection']['postgres_connection_string']
# Connection string to the mongodb
mongo_connection_string = config['connection']['mongo_connection_string']

@op(
    ins={"start": In(bool)}
)

# Visualization of the impact of time of day on crash outcomes
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
            df = sqlio.read_sql_query(text(query_string), connection)

        # Plotting
        p = figure(title="Impact of Time of Day on Crash Outcomes", x_axis_label='Hour of the Day', y_axis_label='Number of Crashes')
        p.vbar(x=df['crash_hour'], top=df['num_crashes'], width=0.9)
        show(p)
    finally:
        engine.dispose()

# Visualization of the impact of age and gender on injury severity
@op
def visualize_age_gender_impact(start: bool):
    # SQL query to fetch data
    query_string = """
    SELECT COALESCE("AGE", -1) AS "AGE", "SEX", "INJURY_CLASSIFICATION", COUNT(*) as cases
    FROM traffic_incidents_table
    WHERE "INJURY_CLASSIFICATION" IS NOT NULL AND "SEX" IN ('M', 'F')
    GROUP BY COALESCE("AGE", -1), "SEX", "INJURY_CLASSIFICATION"
    ORDER BY COALESCE("AGE", -1), "SEX";
    """
    engine = create_engine(postgres_connection_string)
    try:
        with engine.connect() as connection:
            df = pd.read_sql(query_string, connection)

        # Convert AGE to string to ensure compatibility with FactorRange
        df['AGE'] = df['AGE'].astype(str)
        
        # Output to static HTML file
        output_file("age_gender_impact.html")
        # Filter DataFrame for 'M' and 'F' genders
        df_m = df[df['SEX'] == 'M']
        df_f = df[df['SEX'] == 'F']
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
    try:
        with engine.connect() as connection:
            df = sqlio.read_sql_query(query_string, connection)

        output_file("safety_measures_effectiveness.html")

        # Configure the source for Bokeh
        source = ColumnDataSource(df)

        # Create a color mapper for injury classification
        color_mapper = factor_cmap('INJURY_CLASSIFICATION', palette=['green', 'yellow', 'orange', 'red', 'black'], factors=df['INJURY_CLASSIFICATION'].unique())

        # Initialize the figure
        p = figure(title="Effectiveness of Safety Measures on Injury Severity", x_range=df['SAFETY_EQUIPMENT'].unique(), 
                   y_axis_label='Cases', height=400, width=700, tooltips=[("Cases", "@cases")])

        # Add vertical bars to the plot
        p.vbar(x='SAFETY_EQUIPMENT', top='cases', width=0.9, source=source, line_color='white', fill_color=color_mapper)

        # Customize plot aesthetics
        p.xaxis.major_label_orientation = 1.2  # Rotate labels for better legibility
        p.xgrid.grid_line_color = None

        show(p)  # Show the plot in the browser
    finally:
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

        output_file("environmental_impact_on_crashes.html")

        # Creating data sources for the plots
        source = ColumnDataSource(df)

        # Weather Condition Impact Plot
        p1 = figure(title="Impact of Weather Conditions on Crash Frequency", x_range=df['weather_condition'].unique(),
                    x_axis_label='Weather Condition', y_axis_label='Number of Crashes', width=450, height=400)
        p1.vbar(x='weather_condition', top='num_crashes', width=0.9, source=source, color=factor_cmap('weather_condition', palette='Viridis256', factors=df['weather_condition'].unique()))

        # Lighting Condition Impact Plot
        p2 = figure(title="Impact of Lighting Conditions on Crash Frequency", x_range=df['lighting_condition'].unique(),
                    x_axis_label='Lighting Condition', y_axis_label='Number of Crashes', width=450, height=400)
        p2.vbar(x='lighting_condition', top='num_crashes', width=0.9, source=source, color=factor_cmap('lighting_condition', palette='Magma256', factors=df['lighting_condition'].unique()))

        # Customize plot aesthetics
        p1.xaxis.major_label_orientation = 1.2  # Rotate labels for better legibility
        p1.xgrid.grid_line_color = None
        # Customize plot aesthetics
        p2.xaxis.major_label_orientation = 1.2  # Rotate labels for better legibility
        p2.xgrid.grid_line_color = None
        # Show plots in a grid layout
        p = gridplot([[p1, p2]])
        show(p)

    finally:
        engine.dispose()

@op
def visualize_crash_causes(start: bool):
    # Establish a database connection
    engine = create_engine(postgres_connection_string)
    try:
        with engine.connect() as connection:
        # Define the SQL query to fetch data
            query = """
            SELECT prim_contributory_cause, "DRIVER_ACTION", "PHYSICAL_CONDITION", COUNT(*) AS num_cases
            FROM traffic_incidents_table
            GROUP BY prim_contributory_cause, "DRIVER_ACTION", "PHYSICAL_CONDITION"
            ORDER BY prim_contributory_cause, "DRIVER_ACTION", "PHYSICAL_CONDITION";
            """
            
            # Execute the query and load data into a DataFrame
            df = pd.read_sql_query(query, engine)                
            # Plotting the primary contributory causes
            plt.figure(figsize=(10, 8))
            cause_counts = df['prim_contributory_cause'].value_counts()
            sns.barplot(x=cause_counts.values, y=cause_counts.index, palette='viridis')
            plt.title('Frequency of Primary Contributory Causes')
            plt.xlabel('Number of Cases')
            plt.ylabel('Primary Contributory Cause')
            plt.show()

            # Heatmap of Driver Action and Physical Condition by Primary Cause
            pivot_table = df.pivot_table(index='DRIVER_ACTION', columns='PHYSICAL_CONDITION', values='num_cases', aggfunc='sum', fill_value=0)
            plt.figure(figsize=(12, 10))
            sns.heatmap(pivot_table, annot=True, fmt="d", cmap='viridis')
            plt.title('Driver Actions and Physical Conditions by Primary Cause')
            plt.xlabel('Physical Condition')
            plt.ylabel('Driver Action')
            plt.show()

    finally:
       engine.dispose()

@op
def visualize_cell_phone_impact(start: bool):
    engine = create_engine(postgres_connection_string)
    query = """
    SELECT "CELL_PHONE_USE", COUNT(*) AS num_cases
    FROM traffic_incidents_table
    GROUP BY "CELL_PHONE_USE"
    ORDER BY "CELL_PHONE_USE";
    """
    df = pd.read_sql_query(query, engine)
    output_file("cell_phone_impact.html")

    # Handling NaN values and replacing them with 'Unknown'
    df['CELL_PHONE_USE'] = df['CELL_PHONE_USE'].fillna('Unknown')

    source = ColumnDataSource(df)

    p = figure(x_range=df['CELL_PHONE_USE'].unique(), title="Impact of Cell Phone Use on Crash Involvement",
               toolbar_location=None, tools="", height=400)
    p.vbar(x='CELL_PHONE_USE', top='num_cases', width=0.9, source=source,
           line_color='white', fill_color=factor_cmap('CELL_PHONE_USE', palette=['blue', 'green', 'gray'], factors=df['CELL_PHONE_USE'].unique()))
    
    p.xaxis.major_label_orientation = 1.0
    p.xgrid.grid_line_color = None

    show(p)
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

        # Interactive map with Plotly
        fig = px.scatter_geo(df, lat='latitude', lon='longitude', hover_name='location', projection='natural earth')
        fig.show()

    finally:
        engine.dispose()