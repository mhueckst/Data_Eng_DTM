# Data_Eng_DTM

(This repository is a backup for the GCP VM we are using- this is why only Tim and Dan have contributed). 

This is a team project for CS 510 Data Engineering consisting of Dan, Tim, Mahshid, and Max (DTM). 

We created a data pipeline to visalize Portland, Oregon bus (Trimet) data using Google Cloud Project, Apache Kafka, PostgreSQL, linux utilities, and Mapbox.  

First, we consumed an API to fetch the Trimet GPS breadcrumbs. Then, we produced these to a Kafka topic, and subsequently consumed them. We automated this process, 
and we created a Slack notification bot to alert us when the daily data had been produced and consumed from the Kafka topic. 

We transformed the data into a more streamlined version using Pandas, then we added a PostgreSQL database to this pipeline, to store the data. This process was also automated. 

Finally, we used Mapbox to visualize SQL queries of the data, to answer questions like "What is the most northern bus route?", or "What is the longest duration bus route?". 

Here is an example from the visualization: This is all of the buses that passed through the SW 4th and Harrison bus stop in downtown Portland, Oregon, on Monday, 1/16/23, from 9-11am:

![data viz pic](https://github.com/mhueckst/Data_Eng_DTM/blob/master/data%20viz%20pic.png)
