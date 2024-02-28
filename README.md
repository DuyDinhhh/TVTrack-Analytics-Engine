# **Objective**

This project is about turning raw data into useful info. We're figuring out how long people watch different types of TV shows, which shows are the most popular, and what customers like to watch. Based on this, we can suggest ads that match their interests. We're also looking at how often customers watch TV to offer them discounts on shows they enjoy.

- Tech stacks: Pyspark, Python, PostgreSQL, Grafana.

## **Raw data**

- Log data from available JSON files.
- Log data schema

```sh
root
|-- _id: string (nullable = true)
|-- _index: string (nullable = true)
|-- _score: long (nullable = true)
|-- _source: struct (nullable = true)
| |-- AppName: string (nullable = true)
| |-- Contract: string (nullable = true)
| |-- Mac: string (nullable = true)
| |-- TotalDuration: long (nullable = true)
|-- _type: string (nullable = true)
```

<img src="https://github.com/taoxintuyenbo/travinhese/blob/main/assets/logdata.png?raw=true" alt="Alt text" width="500" height="500">

## **Processing data**

- Choose the `["_source"]` column and introduce a new column named `Type`, which includes `TVDuration, RelaxDuration, MovieDuration, ChildDuration` and `SportDuration` based on the `AppName` data.
- Eliminate records with invalid `Contract`, pivot the `Type` category by grouping them under `Contract`, and calculate the total duration.
- Generate a list of date ranges for specific time periods provided by the user via `convert_to_datevalue()` and `date_range` functions for the available files:

<img src="https://github.com/taoxintuyenbo/travinhese/blob/main/assets/file.png?raw=true" alt="Alt text" width="700" height="200">

- Determine the activity level based on the `number_of_days` ETL process; if it's higher than half of `number_of_day`, classify it as `high`; otherwise, label it as `low`.
- Identify the `most-watched` category by selecting the highest value among the columns `TVDuration, RelaxDuration, MovieDuration, ChildDuration` and `SportDuration`.
- Assess customer preferences by setting the taste based on any of the columns `TVDuration, RelaxDuration, MovieDuration, ChildDuration` and `SportDuration` being present and then import data to datawarehouse(postgresql).

## **Clean data**

- Clean data schema:

```sh
root
 |-- Contract: string (nullable = true)
 |-- ChildDuration: long (nullable = true)
 |-- TVDuration: long (nullable = true)
 |-- RelaxDuration: long (nullable = true)
 |-- SportDuration: long (nullable = true)
 |-- MovieDuration: long (nullable = true)
 |-- Active_ness: string (nullable = true)
 |-- MostWatch: string (nullable = true)
 |-- Taste: string (nullable = false)
```

![clean_data](https://github.com/taoxintuyenbo/travinhese/blob/main/assets/cleandata.png?raw=true)

## **Visualizing Data with Grafana**

![visualize_grafana](https://github.com/taoxintuyenbo/travinhese/blob/main/assets/visualizegrafana.png?raw=true)

