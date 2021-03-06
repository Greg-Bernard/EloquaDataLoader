#!/usr/bin/python
# ElqBulk scheduler by Greg Bernard

import schedule
import time
from ElqBulk import ElqBulk
from ElqRest import ElqRest
import TableNames
import geoip
from closest_city import CityAppend


def initialise_database(filename='EloquaDB.db'):
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
    tb = ElqBulk(filename=filename, table=table)
    tb.create_table()
    tb.get_initial_data()
    tb.load_to_database()
    tb.commit()
    tb.close()


def sync_database(filename='EloquaDB.db'):
    """
    Sync entire database in one run
    """

    for item in TableNames.tables:
        sync_table(item, filename)


def sync_table(table, filename='EloquaDB.db'):
    """
    Sync only the data for a single table
    :param table: the name of the table you're syncing from Eloqua
    :param filename: the name of the file you're dumping the data into
    """

    # Only load/update all values for a single table
    tb = ElqBulk(filename=filename, table=table)
    tb.create_table()
    tb.get_sync_data()
    tb.load_to_database()
    tb.commit()
    tb.close()


def sync_tables(tables, filename='EloquaDB.db'):
    """
    Initialize the data for 1 to many tables
    :param tables: the list of the tables you're syncing from Eloqua
    :param filename: the name of the file you're dumping the data into
    """

    if set(tables).issubset(TableNames.tables) is False:
        print("The inputs must be within the accepted list of Eloqua tables.")
        exit()

    for item in tables:
        sync_table(item, filename)


def sync_external_activities(filename='EloquaDB.db', start=None, end=99999):
    """
    Syncs external activities to the database
    :param filename: the name of the file you're dumping the data into
    :param start: number of the record you wish to start you pull from, defaults to last record created
    :param end: number of the last record you wish to pull, non-inclusive
    """

    db = ElqRest(filename=filename, sync='external')
    db.export_external(start=start, end=end)


def sync_campaigns(filename='EloquaDB.db'):
    """
    Syncs campaigns to the database
    :param filename: the name of the file you're dumping the data into
    """

    db = ElqRest(filename=filename, sync='campaigns')
    db.export_campaigns()


def sync_users(filename='EloquaDB.db'):
    """
    Syncs campaigns to the database
    :param filename: the name of the file you're dumping the data into
    """

    db = ElqRest(filename=filename, sync='users')
    db.export_users()


def full_geoip(**kwargs):
    """
    Run geoip on all tables that contain the column IpAddress.
    :param filename: file to sync to
    :param tables_with_ip: list of tables containing IP Addresses to cycle through
    """
    tables_with_ip = kwargs.get('tables_with_ip', ['EmailClickthrough', 'EmailOpen', 'PageView', 'WebVisit'])
    filename = kwargs.get('filename', 'EloquaDB.db')

    for tb in tables_with_ip:
        run_geoip(filename=filename, tablename=tb)


def run_geoip(**kwargs):
    """
    Runs the IP lookup on specified tables that creates a table indexing all
    IP Address Geolocations where at least the city was provided
    :param filename: file to sync to
    :param tablename: table to take IP Addresses from to geolocate
    """
    table = kwargs.get('table','EmailClickthrough')
    filename = kwargs.get('filename', 'EloquaDB.db')

    db = geoip.IpLoc(filename=filename, tablename=table)
    db.create_table()
    db.save_location_data()
    db.commit_and_close()


def closest_city(**kwargs):
    """
    Takes every coordinate in the GeoIP table and calculates the closest city against every major population center in NA
    :param kwargs: table = name of the table (GeoIP), filename = name of database file (EloquaDB.db)
    """

    table = kwargs.get('table', 'GeoIP')
    filename = kwargs.get('filename', 'EloquaDB.db')

    cc = CityAppend(filename=filename, table=table)
    cc.closest_cities()
    cc.load_to_database()


def daily_sync(**kwargs):
    """
    Schedule a sync every day at specified time, default to midnight
    :param daytime: which time of day to perform the sync Format: hh:mm
    :param sync: which sync function to perform
    :param filename: file to sync to
    """
    daytime = kwargs.get('daytime', "00:00")
    filename = kwargs.get('filename', 'EloquaDB.db')
    sync = kwargs.get('sync', sync_database(filename=filename))

    print("Scheduling a daily Eloqua sync at {}.".format(daytime))
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
    filename = kwargs.get('filename', 'EloquaDB.db')
    sync = kwargs.get('sync', sync_database(filename=filename))

    print("Scheduling an Eloqua sync every {} hours.".format(hours))
    schedule.every(hours).hours.do(sync)

    while True:
        schedule.run_pending()
        time.sleep(1)


def available_tables():
    """
    Return available table names for export.
    """
    print(TableNames.tables)


def main(filename='EloquaDB.db'):
    """
    Main function runs when file is run as main.
    """

    # Performs full database sync, only updating records modified since the last sync
    sync_database(filename=filename)

    # Iterates through all tables with IP addresses and logs the IP with
    # its geolocation in the GeoIP table
    full_geoip(filename=filename)

    # Calculates the distance from a given point to every major population center in North America
    # Then returns that population center, the distance from it in km, and the country that city is in
    closest_city(filename=filename)

    # Performs a full sync of all users in Eloqua
    sync_users(filename=filename)

    # Performs a full campaign sync, updates the last 'page' of campaigns (default page size is set to 100)
    sync_campaigns(filename=filename)

    # Performs full external activity sync, only updating records created since the last sync
    # WARNING THIS CAN USE A HIGH NUMBER OF API CALLS AND TAKE A LONG TIME - CHECK YOUR API LIMIT BEFORE USING THIS
    sync_external_activities(filename=filename)

    # Exports GeoIP table inner joined with tables that contain activities
    # with IP addresses in csv format
    geoip.export_geoip(filename=filename)


# When using schedulers
# To clear all functions
# schedule.clear()

# if this module is run as main it will execute the main routine
if __name__ == '__main__':
    main()
