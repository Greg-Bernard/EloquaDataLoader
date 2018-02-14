# EloquaDataLoader
Export data from Oracle Eloqua's BULK API using the Python Eloqua API wrapper made by Jeremiah Coleman ([pyeloqua](https://pypi.python.org/pypi/pyeloqua/0.5.6)) then either sync it into an SQlite3 database or dump into JSON files.

## Basics: Using the script
To use the script fill in your Eloqua login information in **config** module, then run the functions located in the **ldbs** module to perform the various syncs available.

You can use the hourly_sync, or daily_sync functions to run any of the scripts in this module at the intervals you specify.

### Module Breakdown:
* **ElqBulk** - The core module that holds the ElqBulk class which performs BULK API 2.0 exports and syncs to your SQLite database, or dumps to JSON
* **ElqRest** - A custom wrapper for the Eloqua REST 2.0 API to import any, or all External Activities in your Eloqua instance.
* **TableNames** - The list of tables currently available for export through BULK API in Eloqua
* **config** - Company, username, and password used to log in to allow ElqDB to function, requires a user with Advanced Marketing User privileges or higher
* **ldbs** - This is the module you'll be running most of the time, it has functions that facilitate the majority of syncing actions available through this script
* **geoip** - An additional module that holds another class that uses the maxminddb package with the GeoLite2 database to geolocate IP addresses located in the activity tables exported with ElqDB

### Usage:

After setting up the config module, open the **ldbs** module and place the functions you want to run in the main function at the bottom.
You can set up any kind of sync you'd like in this module, as well as run most functions from any module in this program.

```python
     def main():
    
       # syncs the entire activity, contact, and account content of your
       # Eloqua instance to the database file specified
       sync_database(filename='EloquaDB.db')
       
       # geolocates every IP with at lest enough resolution to determine city
       full_geoip(filename='EloquaDB.db')
       
       # exports all activities with IP addresses and their geolocations to CSV files name after their tables
       geoip.export_geoip(filename='EloquaDB.db') 
 ```

## Geolocation By IP
Added functionality provided through the geoip module. Use the *run_geoip* or *full_geoip* functions in **ldbs** to roughly match the IP Addresses in activity tables that contain them with real-world coordinates. Accuracy of these coordinates vary from 5km to 50km, so only really useful for high level anaylsis/insights. 

## Dependencies
* [pyeloqua](https://pypi.python.org/pypi/pyeloqua/0.5.6)
* [maxminddb](https://pypi.python.org/pypi/maxminddb)
* [schedule](https://pypi.python.org/pypi/schedule)
* [maxminddb GeoLite2 Database File](https://dev.maxmind.com/geoip/geoip2/geolite2/)

*Download the GeoLite2 City MaxMind DB binary, gzipped file, then unpack it in the same directory as your .py files.*
