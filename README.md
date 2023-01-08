# English Premier League Fantasy Premier League Project

#### Objective:
The aim of this project is to create an interactive dashboard to assist players of Fantasy Premier League (FPL) to make informed decsions and increase their performance in the game. The project will use an ETL data pipeline to automatically update the dashboard daily with the latest statistics for the EPL.

#### Tasks:
The project is being undertaken by three different students, each student has a different task:

- Student 1 = Obtain FPL data from the FPL API avialable at 
- Student 2 = Scrape injury history for players currently playing in the English Premier League (EPL) from Transfermarkt.com
- Student 3 = Obtain all the latest injury updates for players playing in the EPL available at 

#### Pipeline
Below is a schematic of the pipeline for the project:

<img width="1010" alt="Screenshot 2023-01-07 at 11 00 06" src="https://user-images.githubusercontent.com/118215055/211216717-04fd0673-ace5-47b3-af4b-f5112e0cc027.png">

The programming language used is Python 3.8, the data lake and data warehouse use Amazon RDS PostgreSQL databases, Apache Airflow is used as the scheduler to move the data through the pipeline and Tableau is used to make the dashboards which can be found at the following links:

#### Student 2 - Repository:
This repository contains the scripts used for Student 2 in order to scrape the injury history data for EPL players from Transfermarkt.com. The structure of the folders are below. *Note: The folder '_airflow_dags' contains all the Apache Airflow DAGs required to scrape the data from the source, merge with the other sources in the project and create a star schema. The folder 'scraping_scripts' contains the scripts to scrape the source and transform the data.*

```bash

├── .gitignore
├── README.md
├── _scraping_scripts
│   ├── 1_scrape_team_urls.py
│   ├── 2_scrape_player_urls.py
│   ├── 3_scrape_player_injuries.py
│   ├── 4_transform_player_injuries.py
|   └──_Functions
|       ├── function_clean_date.py
|       ├── function_player_bios.py
|       └── function_player_stats.py
├── _ariflow_dags
│   ├── 1_scrape_team_urls_DAG.py
│   ├── 2_scrape_player_urls_DAG.py
│   ├── 3_scrape_player_injuries_DAG.py
│   ├── 4_transform_player_injuries_DAG.py
│   ├── 5_dw_injuries_DAG.py
│   ├── 6_dw_performance_DAG.py
|   └──_Functions
|       ├── function_clean_date.py
|       ├── function_player_bios.py
|       └── function_player_stats.py
└── .DS_Store

```
