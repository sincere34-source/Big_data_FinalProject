# Big Data Analytics Final Project

This project demonstrates an end-to-end big data analytics pipeline using
MongoDB, HBase, and Apache Spark.

## Project Structure

- data_generation.py  
  Generates or prepares the raw JSON datasets used in the project.

- mongodb_analysis.py  
  MongoDB schema design and aggregation queries for transactional analytics.

- hbase_schema.py  
  HBase table design, row-key strategy, and sample session queries.

- spark_analytics.py  
  Batch analytics, Spark SQL queries, product affinity analysis,
  and data preparation for visualization.

## Technologies Used
- MongoDB
- Apache HBase
- Apache Spark (PySpark)
- Python (Matplotlib for visualization)

## How to Run
1. Generate or load data using `data_generation.py`
2. Run MongoDB queries using `mongodb_analysis.py`
3. Create and query HBase tables using `hbase_schema.py`
4. Run analytics using `spark_analytics.py`

All scripts are well-commented and correspond directly to the sections
described in the technical report.
