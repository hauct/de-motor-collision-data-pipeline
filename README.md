# Motor Collision Data Project

## Overview

The [Data Engineering Zoomcamp](https://github.com/hauct/de-zoomcamp) course, facilitated by [DataTalks.Club](https://datatalks.club/), included the completion of this project.


The primary objective is to utilize the skills acquired during the course to establish a comprehensive data pipeline. The project has been structured in both local and cloud variants.

Project aims include:

  * Constructing a full-fledged data pipeline for batch data processing;
  * Creating dbt models to curate data aligned with analytical needs;
  * Designing an analytical interface to easily recognize patterns and understand insights;
  * Crafting a script to tailor the dashboard based on a specific template;
  * Offering two technological implementations: local and on the cloud.

Data for processing spans from the year 2012 to 2023.

## Datasets used in the project

[Motor Vehicle Collisions - Crashes](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95).
This data provides insights into every traffic collision incident occurring on the streets of New York City. Each entry corresponds to an individual crash. This dataset encompasses details from every motor vehicle collision in NYC as reported by the police. A police report (MV104-AN) becomes obligatory when there are injuries or fatalities, or the damage is valued at $1000 or more.

[Motor Vehicle Collisions - Vehicles](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Vehicles/bm4k-52h4).
This dataset offers specifics about each vehicle that was part of the collision. Every entry here signifies a vehicle in an incident.

[Motor Vehicle Collisions - Person](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Person/f55k-p6yu).
This dataset documents information regarding the individuals involved in these collisions. Each entry in this dataset denotes an individual, whether they're a driver, passenger, pedestrian, cyclist, and so on, associated with an incident.

All the information is directly sourced from the electronic crash reporting system, omitting any data that could reveal personal identities.

## Problem description and purposes

This initiative focuses on Motor Vehicle Collisions in New York, drawing from three distinct datasets.

The analysis aims to uncover the car types most frequently involved in accidents, ascertain the reasons behind these collisions, determine the peak times for accidents, and profile the age and gender of the drivers implicated.

It's also essential to gauge the completeness of the datasets, identifying unknown entries concerning vehicles, individuals, and other relevant data.

To achieve these objectives in a cloud environment, the following steps are needed:
 
  * Design a template for seamless initiation of the required cloud services, including elastic compute cloud, datalake, and data warehouse.
  * Establish a data pipeline to extract raw input from sources, modify it, then store it in the datalake, and subsequently move it to the data warehouse.
  * Organize the uploaded data by year.
  * Consolidate data for analytical purposes:
    * annually
    * based on daily hours
    * highlighting gaps in data
  * Design a comprehensive dashboard representing the consolidated data.
  * Formulate a script for automatic dashboard generation.

## Technologies used in Local version

  * Containerization: Docker
  * Data Base: PostreSQL
  * Batch Orchestration: Prefect + Python
  * Data Transformation and Generalization : DBT
  * Visualisation: Metabase

## Project architecture (Local version)

  * Creating PostgreSQL database and environment (docker)
  * Downloading datasets from source (prefect + python)
  * Transformation and loading data into the database (prefect + python)
  * Transformation, modeling and generalization of data into a database (dbt)
  * Visualization of transformed and generalized data (metabase)


