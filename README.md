# EloquaDataLoader
Export data from Oracle Eloqua's BULK API using the Python Eloqua API wrapper made by Jeremiah Coleman ([pyeloqua](https://pypi.python.org/pypi/pyeloqua/0.5.6)) then either sync it into an SQlite3 database or dump into JSON files.

## Basics: Using the script
To use the script fill in your Eloqua login information in **config** file, then run the functions located in the **ldbs** file to perform the various syncs available.

You can use the hourly_sync, or daily_sync functions to run any of the scripts in this file at the intervals you specify.

### File Breakdown:
* **ElqDB** - The core file that holds the ElqDB class which performs the Exports, syncs to your SQLite database, and dumps to JSON
* **TableNames** - The list of tables currently available for export through BULK API in Eloqua
* **confi** - Company, username, and password used to log in to allow ElqDB to function, requires a user with Advanced Marketing User privileges or higher
* **ldbs** - This is the file you'll be running most of the time, it has functions that facilitate the majority of syncing actions available through this script
* **geoip** - An additional file that holds another class that uses the maxminddb package with the GeoLite2 database to geolocate IP addresses located in the activity tables exported with ElqDB

## Geolocation By IP
Added functionality provided through the geoip file. Use the *run_geoip* or *full_geoip* functions in **ldbs** to roughly match the IP Addresses in activity tables that contain them with real-world coordinates. Accuracy of these coordinates vary from 5km to 50km, so only really useful for high level anaylsis/insights. 

## Dependencies
* [pyeloqua](https://pypi.python.org/pypi/pyeloqua/0.5.6)
* [maxminddb](https://pypi.python.org/pypi/maxminddb)
* [schedule](https://pypi.python.org/pypi/schedule)
* [maxminddb GeoLite2 Database File](https://dev.maxmind.com/geoip/geoip2/geolite2/)

*Download the GeoLite2 City MaxMind DB binary, gzipped file, then unpack it in the same directory as your .py files.*
