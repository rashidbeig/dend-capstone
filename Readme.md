# Project Title
### Data Engineering Capstone Project

#### Project Summary
    The key objective of this project is build and ETL pipeline to create an analytics database for arrival and departure data to US, world temperatures and US demgraphics. This analytics database can be used to analyse the trends and answer questions whether there is a corelation between temperature of countries globally with the number of people immigrating to US from these countries and so on.
    
#### Files
    1. Capstone Project Template.ipynb: Jupyter Notebook that is used to build the pipeline.
    2. etl_functions.py : Code to create fact and dimension tables.
    3. etl.py : Code for data cleaning and visualisation.
    4. config.cfg : AWS Credentials
    5. Questions.ipynb : Jupyter Notebook that is used to build queries to ask questions 

The project follows the follow steps:

* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

### Step 1: Scope the Project and Gather Data

#### Scope 
The Scope of this project is to process the immigration datasets stored in Amazon S3 using Apache Spark. More specifically following tasks will be done:
1. Use Spark to create Pandas dataframes.
2. Explore the raw datasets.
3. Clean the data.
4. Create fact and dimension tables.

#### Describe and Gather Data 

    * I94 Immigration Data: This data comes from the US National Tourism and Trade Office found here. Each report contains international visitor arrival statistics by world regions and select countries (including top 20), type of visa, mode of transportation, age groups, states visited (first intended address only), and the top ports of entry (for select countries).

    * World Temperature Data: This dataset came from Kaggle found here.

    * U.S. City Demographic Data: This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. Dataset comes from OpenSoft found here.

    * Airport Code Table: This is a simple table of airport codes and corresponding cities. The airport codes may refer to either IATA airport code, a three-letter code which is used in passenger reservation, ticketing and baggage-handling systems, or the ICAO airport code which is a four letter code used by ATC systems and for airports that do not have an IATA airport code (from wikipedia). It comes from here.
    
    
### Step 2: Explore and Assess the Data

#### Explore the Data 
Identify data quality issues, like missing values, duplicate data, etc.

#### Cleaning Steps
Document steps necessary to clean the data

#### Data Cleaning steps:

    1. Drop columns with over 90% missing values. 

    2. Drop all rows with 100% missing values.

    3. Drop columns with significant missing values.
    
### Step 3: Define the Data Model

#### 3.1 Conceptual Data Model
    
   ### Database schema
   
   We have used Star schema to map the data. The immigration_fact table is the center of this schema that is connected country_dim, usa-demographics_dim and        immigration_calendar_dim dimension tables through foreign keys.This schema with fewer foreign keys helps to query data faster, makes the queries simpler and    helps in high data redundancy. By connecting the tables and data in this way we can easily answerer questions like :
   1. On which dates were the arrivals more from different countries ?
   2. Which states in USA had more arrivals from a particular country ?
   3. Which visa types people had in different seasons ?
    
   #### Dimension Tables
    
    country_dim : has data from the global land temperatures by city and the immigration datasets. 
    
    usa_demographics_dim :has data from US states with respect to its demographics.

    visa_type_dim: has data from immigration datasets and links to the immigaration via the visa_type_key.
    
    immigration_calendar_dim: custom table that has arrival dates based on particular date and time.
    
   #### Fact Tables
    
    immigration_fact:  has data from the immigration data sets and contains keys that links to the dimension tables. 
    

#### 3.2 Mapping Out Data Pipelines

     The pipeline steps are as follows:

     1. Load the datasets
     2. Clean the I94 Immigration data to create Spark dataframe for each month
     3. Create visa_type dimension table
     4. Create calendar dimension table
     5. Extract clean global temperatures data
     6. Create country dimension table
     7. Create immigration fact table
     8. Load demographics data
     9. Clean demographics data
    10. Create demographic dimension table
    

### Step 4: Run Pipelines to Model the Data 

#### 4.1 Create the data model
Build the data pipelines to create the data model.


#### 4.2 Data Quality Checks
Explain the data quality checks you'll perform to ensure the pipeline ran as expected. These could include:
 * Integrity constraints on the relational database (e.g., unique key, data type, etc.)
 * Unit tests for the scripts to ensure they are doing the right thing
 * Source/Count checks to ensure completeness
 
Run Quality Checks