********************** TASK 1
1. Your task is to create a data processing pipeline in Python that matches and normalizes the datasets. The output should be a cleaned, unified dataset that is ready for analysis. You must ensure that it is scalable to work with multiple batches of data.
   data_processing_pipeline.py - It's a python script that uses mainly pandas for TEL process. I would prefer user dask (considering large volume data from brokerchooser) or even and most convenient for myself Apache Spark.


********************** TASK 2
2. Draw an ideal pipeline and write recommendations on how the ETL pipeline could be more accurate & automatized. 
   What other tools would you use in this task?
   
   Ingestion Layer:
      Use cloud storage like ADLS Gen2 to store the incoming datasets. For batch processing adn a good treatment of ingestion data I would
      use Azure Data FActory. This way I would make a pipeline with 3 data flows, infering the proper schema. 
      I would output the data to Delta Lake staging table in Azure Databricks for the next transformation part.

   Transformation Layer:
      Normalize, merge and clean data using Python (pandas or Dask for large datasets). 
      Another way would be using Apache Spark for distributed processing when dealing with large volumes of data. 
      This could help scalibily problems and it's an easy way to handle large datasets efficiently.

   Data Output Layer:
      Store the cleaned, unified dataset in a data warehouse like dedicated SQL pool in Azure Synapse Analytics, which allows to analyze data with visualization tools such as Power BI.

   Orchestration:
      Use Apache Airflow to orchestrate and automate the entire ETL pipeline. It's useful for scheduling, dependency management, and error handling.

   Recommendations for Accuracy:
      Data Quality Checks: Implement data validation at each stage of the pipeline to ensure data integrity.
      Version Control: Store versions of datasets to track changes and rollback if necessary.


********************** TASK 3
3. Do an exploratory analysis of the matched data and bring insights and action steps to the product of BrokerChooser.
   data_pipeline_users - It's a script that analyzes users data, but the idea is to obtain conclusions with same treatments from the output dataframe.