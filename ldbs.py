#!/usr/bin/python
# ElqDB scheduler by Greg Bernard

import schedule
import time
from ElqDB import ElqDB
import TableNames
import geoip


def initialise_database(filename='ElqData.db'):
    """
    Initialise entire database in one run
    """

    for item in TableNames.tables:
        initialise_table(item, filename)


def initialise_table(table, filename='ElqData.db'):
    """
    Initialise only the data for a single table
    :param table: the name of the table you're syncing from Eloqua
    :param filename: the name of the file you're dumping the data into
    """

    # Only load/update all values for a single table
    tb = ElqDB(filename=filename, table=table)
    tb.create_table()
    tb.get_initial_data()
    tb.load_to_database()
    tb.commit()
    tb.close()


def sync_database(filename='ElqData.db'):
    """
    Sync entire database in one run
    """

    for item in TableNames.tables:
        sync_table(item, filename)


def sync_table(table, filename='ElqData.db'):
    """
    Sync only the data for a single table
    :param table: the name of the table you're syncing from Eloqua
    :param filename: the name of the file you're dumping the data into
    """

    # Only load/update all values for a single table
    tb = ElqDB(filename=filename, table=table)
    tb.create_table()
    tb.get_sync_data()
    tb.load_to_database()
    tb.commit()
    tb.close()


def sync_tables(tables, filename='ElqData.db'):
    """
    Initialize the data for 1 to many tables
    :param tables: the list of the tables you're syncing from Eloqua
    :param filename: the name of the file you're dumping the data into
    """

    if set(tables).issubset(TableNames.tables) is False:
        print('The inputs must be within the accepted list of Eloqua tables.')
        exit()

    for item in tables:
        sync_table(item, filename)


def full_geoip(**kwargs):
    """
    Run geoip on all tables that contain the column IpAddress.
    :param filename: file to sync to
    :param tables_with_ip: list of tables containing IP Addresses to cycle through
    """
    tables_with_ip = kwargs.get('tables_with_ip', ["EmailClickthrough", "EmailOpen", "PageView", "WebVisit"])
    filename = kwargs.get('filename', 'ElqDB.db')

    for tb in tables_with_ip:
        run_geoip(filename=filename,tablename=tb)


def run_geoip(**kwargs):
    """
    Runs the IP lookup on specified tables that creates a table indexing all
    IP Address Geolocations where at least the city was provided
    :param filename: file to sync to
    :param tablename: table to take IP Addresses from to geolocate
    """
    tablename = kwargs.get('tablename','EmailClickthrough')
    filename = kwargs.get('filename', 'ElqDB.db')

    db = geoip.IpLoc(filename=filename,tablename=tablename)
    db.ip_data()
    db.process_step()
    db.create_table()
    db.save_location_data()
    db.commit_and_close()


def daily_sync(**kwargs):
    """
    Schedule a sync every day at specified time, default to midnight
    :param daytime: which time of day to perform the sync Format: hh:mm
    :param sync: which sync function to perform
    :param filename: file to sync to
    """
    daytime = kwargs.get('daytime', "00:00")
    filename = kwargs.get('filename', 'ElqDB.db')
    sync = kwargs.get('sync', sync_database(filename=filename))

    print('Scheduling a daily Eloqua sync at {}.'.format(daytime))
    schedule.every().day.at(daytime).do(sync)

    while True:
        schedule.run_pending()
        time.sleep(1)


def hourly_sync(**kwargs):
    """
    Schedule a sync every set number of hours
    :param hours: how many hours to wait between syncs
    :param sync: which sync function to perform
    :param filename: file to sync to
    """
    hours = kwargs.get('hours', 4)
    filename = kwargs.get('filename', 'ElqDB.db')
    sync = kwargs.get('sync', sync_database(filename=filename))

    print('Scheduling an Eloqua sync every {} hours.'.format(hours))
    schedule.every(hours).hours.do(sync)

    while True:
        schedule.run_pending()
        time.sleep(1)


def available_tables():
    """
    Return available table names for export.
    """
    print(TableNames.tables)


def main():
    """
    Main function runs when file is run as main.
    """

    # Performs full database sync, only updating records modified since the last sync
    sync_database(filename="EloquaDB.db")

    # Iterates through all tables with IP addresses and logs the IP with
    # its geolocation in the GeoIP table
    full_geoip(filename="EloquaDB.db")
    
    # Exports GeoIP table inner joined with tables that contain activities
    # with IP addresses in csv format
    geoip.export_geoip(filename="EloquaDB.db")

# When using
# To clear all functions
# schedule.clear()


# if this module is run as main it will execute the main routine
if __name__ == "__main__":
    main()
