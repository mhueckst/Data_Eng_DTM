Data_Eng_DTM

Project Overview
Data_Eng_DTM is a team project developed for CS 510 Data Engineering, consisting of team members Dan, Tim, Mahshid, and Max (DTM).In this project, we delve into the complex task of constructing a data pipeline for TriMet's GPS breadcrumb data. Our goal is to not only gather and transport this data but also validate, store, and integrate it with additional sources for a comprehensive visualization of Portland, Oregon's public transportation system.

Understanding TriMet
TriMet operates a public transportation network in and around Portland, Oregon. Our focus is on TriMet's GPS breadcrumb data, which records the geographical positions of all buses in the system at 5-second intervals. This rich dataset provides a foundation for in-depth analysis of the city's bus operations.

Key functionalities include:
- Consuming API to fetch TriMet GPS breadcrumbs.
- Producing and consuming data to/from a Kafka topic.
- Automating data processing and notifications via a Slack bot.
- Transforming data using Pandas and storing it in PostgreSQL.
- Visualizing SQL queries of the data on Mapbox.

 Requirements
- Python 3.10.9 (Conda base environment)
- Additional Python libraries as listed in `requirements.txt`

Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/Mahshiiiiid/CRR_Final_Project.git
   ```
2. Navigate to the project directory:
   ```bash
   cd Data_Eng_DTM
   ```
3. Install required Python packages:
   ```bash
   pip install -r requirements.txt
   ```

Configuration
- Configure the environment variables as needed in `.env` file.
- Ensure proper setup for Apache Kafka and PostgreSQL.

Running the Application
Execute the following scripts in order to set up and run the data pipeline:

1. Gather and Produce Data:
   ```bash
   python gather_produce.py <path to getting_started.ini>
   ```

2. Consume Data:**
   - For initial consumption:
     ```bash
     python consumer.py <path to getting_started.ini>
     ```
   - For updated consumption process:
     ```bash
     python new_consumer.py <path to getting_started.ini>
     ```

3. Load Data to PostgreSQL:**
   - For trips data:
     ```bash
     python load_to_postgres_trips.py
     ```
   - For general data loading:
     ```bash
     python load_to_postgres.py
     ```

4. Data Transformation and Validation:**
   ```bash
   python validate_transform.py
   ```

5. Initiating Database Load:**
   ```bash
   python initiate_db_load.py
   ```

6. Server and Visualization:**
   - Start the server:
     ```bash
     python server.py
     ```
   - Access the visualization through `index.html`.

Testing
Run the following command to execute tests (if any):
```bash
python -m unittest
```
Additional Project Details

Data Source: TriMet GPS breadcrumb and stop event data.
GCP VMs: Utilized for various pipeline components.
Kafka System: Confluent Kafka for data streaming.
Producers and Consumers: Integrated within the Kafka system.
Database Server: PostgreSQL for data storage.
Special Considerations: Includes validations and transformations for breadcrumb and stop data to ensure data integrity and accuracy.
Contributing
Contributions to the Data_Eng_DTM project are welcome. Please follow the standard fork and pull request workflow.

Contact
For any queries or contributions, please contact mghasemi@pdx.edu.


