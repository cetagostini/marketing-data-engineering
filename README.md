# Marketing Data Engineering
Within this project I will study the way to create architectures that can collect data in a simple way, and be used in different visualization sources such as Data Studio, Tableau, Power BI or Looker.

## Data Architecture
The structure designed to maintein the dashboard on a daily basis works using components from Google Cloud Platform and Amazon Web Services. While the server, databases and buckets are inside Amazon, the APIs and visualization service work on Google Cloud Platform.

Additionally, we have integrations with Github to be able to generate version control within the code that we have in production and SuperMetrics as the main integration for extracting data from different sources such as Facebook ads, Google ads or Google Analytics. However, the extraction can be replaced by any other method instead of Google Sheets.

### High level description of the process 💡
1. We collect data from different sources and put it into Google Sheets, this information is refreshed every hour. You can use tools as Supermetrics to make this and sintetize all your analytics data sources in one single place.

2. Subsequently, our codes stored on GitHub are executed by our server (Using <Curl -s> command) to collect the data from the Google Sheets. This information is cleaned, enriched and organized to be deposited in S3.
  > curl -s https://rawfile/path/myfile.py | python - here_can_use_parameters

3. Finally the information is taken from S3 and sent to Redshift to be read by data studio in the aforementioned dashboard.
