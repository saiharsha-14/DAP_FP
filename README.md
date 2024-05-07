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

   1. config.yaml       ---    all connection strings need to be edited here
   2. pre.txt           ---    Containes all the required libraries to be installed before running.
   3. main.py           ---    Extracting data from downloaded files and ingesting it to MongoDB.
   4. transform.py      ---    Transforming and loading the extracted data to postgresql.
   5. vis.py            ---    Contains all the visualizations used to answer the research questions.
   6. respository.py    ---    It executes all the OP's as a Pipeline.

Note : - Install all the libraries mention in pre.txt with "pip install -r pre.txt" before executing the respository.py Modify the username and pasword along with db name in both MongoDB and Postgresql before executing in config.yaml file

#### run the dagster
In our project, Dagster is the key role for our ETL processes, facilitating a fully automated pipeline that streamlines every step from data acquisition to storage. Dagster seamlessly manages the ingestion of data. It further automates the extraction, transformation, and loading (ETL) phases, ensuring that the processed data is efficiently transferred into our PostgreSQL database. This robust automation allows the entire data workflow to be controlled by a single command in python virtual environment which we added below,  making the process both efficient and user-friendly. This setup not only saves time but also enhances the reliability and reproducibility of our data analysis pipeline.

```bash
dagit -f respository.py  
