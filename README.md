# DAP-Project
## Semester 1 - Database and Analytical Programming Project

### Project Overview
The Python-based data analytics project intends to help transfer the management of data between MongoDB and PostgreSQL databases. By osing versatile Python scripts to obtain and manipulate information, it allows natural switching between non-relational and relational databases . More so, the project allows leveraging of the PostgreSQL data to create dynamic Python visualizations. As a result of the majority of data is available for analysis, data transformation and extraction are common..

### Key Features
- **Data Extraction and Loading**: High-quality scripts to get and load the desired data from MongoDB into PostgreSQL.
- **Data Transformation**: BCD cleaning and transformation, ensuring that the information is useful and meaningful.
- **Interactive Visualizations**: Utilization of Python visualization libraries to analyze data and generate insights dynamically.
- **Dagster Integration**: The integration acquisition for flow orchestration; it manages dependency links, allowing to have no failures in the datasets and maintaining robust data pipelines.

### Technologies Used
- **Python**: Primary programming language for scripting and automation.
- **MongoDB**: NoSQL database used for storing unstructured data.
- **PostgreSQL**: SQL database employed for structured data storage.
- **Dagster**: Workflow orchestration tool to manage pipelines efficiently.

### Getting Started
#### Prerequisites
- Python 3.10
- MongoDB installed and running on localhost (default port 27017)
- PostgreSQL installed and running on localhost (default port 5432)
- Python libraries: `pymongo`, `psycopg2-binary`, `pandas`, `matplotlib`

#### Installation
1. **Clone the repository:**
   ```bash
   git clone https://github.com/saiharsha-14/DAP_FP.git
   cd DAP_FP