Data_Eng_DTM

Project Overview
Data_Eng_DTM is a team project developed for CS 510 Data Engineering, consisting of team members Dan, Tim, Mahshid, and Max (DTM). This project implements a data pipeline to visualize Portland, Oregon bus (TriMet) data utilizing Google Cloud Project, Apache Kafka, PostgreSQL, Linux utilities, and Mapbox.

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
  - You will need an OAuth token from the Slack API. You can get started here: https://api.slack.com/
  - You will also need the name of a Slack channel you want notifications from this program to go to.
- Ensure proper setup for Apache Kafka and PostgreSQL.
  - https://kafka.apache.org/documentation/
  - https://www.postgresql.org/about/

Running the Application
Execute the following scripts in order to set up and run the data pipeline:

1. Gather and Produce Data:

   ```bash
   python gather_produce.py
   ```

2. Consume Data:\*\*

   - For initial consumption:
     ```bash
     python consumer.py
     ```
   - For updated consumption process:
     ```bash
     python new_consumer.py
     ```

3. Load Data to PostgreSQL:\*\*

   - For trips data:
     ```bash
     python load_to_postgres_trips.py
     ```
   - For general data loading:
     ```bash
     python load_to_postgres.py
     ```

4. Data Transformation and Validation:\*\*

   ```bash
   python validate_transform.py
   ```

5. Initiating Database Load:\*\*

   ```bash
   python initiate_db_load.py
   ```

6. Server and Visualization:\*\*
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

Contributing
Contributions to the Data_Eng_DTM project are welcome. Please follow the standard fork and pull request workflow.

Contact
For any queries or contributions, please contact mghasemi@pdx.edu.
