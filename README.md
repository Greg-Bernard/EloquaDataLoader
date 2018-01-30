# EloquaDataLoader
Export data from Oracle Eloqua's BULK API using the Python Eloqua API wrapper made by Jeremiah Coleman ([pyeloqua](https://pypi.python.org/pypi/pyeloqua/0.5.6)) then either sync it into an SQlite3 database or dump into JSON files.

## Basics: Using the script
To use the script fill in your Eloqua login information in config file, then run the functions located in the ldbs file to perform the various syncs available.

## Geolocation By IP
Added functionality provided through the geoip file. Use the run_geoip or full_geoip functions in ldbs to roughly match the IP Addresses in activity tables that contain them with real-world coordinates. Accuracy of these coordinates vary from 5km to 50km, so only really useful for high level anaylsis. 
