Python Flask APP (ETL)

### What is ETL_FLASKR
is flask app that read excel files from a folder and create PostgreSQL Database tables
with predefined schema and push the data into that PostgreSQL tables, the app handle unexpected errors
and invalid types and apply all fixes needed to push the data into the all tables with 100% success rate
the app apply small analysis while pushing the data to trace the progress and any failure or ignored data
at the end the app return JSON report with information about success percentage for each file and what is
rows ignored or it may have issue


#### How it Works

* the app read excel file from Folder static/excel (and extract the data from the files)
```
['countries.csv', 'currency_details.csv', 'fx_rates.csv', 'transactions.csv', 'users.csv', 'fraudsters.csv']
```
* if files replaced the app will read the updated files, then it will drop all tables and recreate with new data
* column names case insensitive
* columns order does not matter the APP use smart function to read the needed data
* !note if file have index column that required like  fx_rates or fraudsters the index must placed in beginning
otherwise any all columns can be changed their order

### the web API

the app have simple web API that return data in JSON format

1. countries_api
2. currency_details_api
3. fx_rates_api
4. transactions_api
5. users_api
6. fraudsters_api

!note API will convert the time values to String 


# final result

![screenshot](https://github.com/MahmoudHegazi/ETL_Flask_APP/blob/main/flaskr/static/excel/screen1.JPG?raw=true)
