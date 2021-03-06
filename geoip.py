#!/usr/bin/python
# IpLoc by Greg Bernard

import sqlite3
import maxminddb
import csv
import time

tables_with_ip = ['EmailClickthrough', 'EmailOpen', 'PageView', 'WebVisit']


class IpLoc:

    def __init__(self, **kwargs):

        self.tablename = kwargs.get('tablename', 'EmailClickthrough')
        self.filename = kwargs.get('filename', 'EloquaDB.db')
        self.database = kwargs.get('database', 'GeoLite2-City.mmdb')

        self.db = sqlite3.connect(self.filename, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        self.db.row_factory = sqlite3.Row
        c = self.db.cursor()

        self.reader = maxminddb.open_database(self.database)

        try:
            sql_data = c.execute('SELECT IpAddress FROM {}'.format(self.tablename))
            self.raw_ip_data = sql_data.fetchall()
        except sqlite3.OperationalError:
            print("ERROR: There is no IpAddress column in this table.")
            exit()

        self.geo_data = self.ip_data()
        self.new_data = self.process_step()
        self.columns = self._create_db_columns_def_()

    def ip_data(self):
        """
        Get available location information for provided IP Addresses in SQlite Row format
        """
        geo_data = []

        print("Retrieving IP locations from the GeoLite2 data set.")
        for ip in self.raw_ip_data:
            try:
                d = self.reader.get(ip['IpAddress'])
                d['IpAddress'] = ip['IpAddress']
                geo_data.append(d)
            except ValueError:
                continue
            except TypeError:
                continue

        return geo_data

    def process_step(self):
        """
        Steps to process the raw data output from ip_data and make it suitable for analysis
        """
        print("Processing GeoLite2 export data for the database.")

        new_data = []

        for dictionary in self.geo_data:
            foo_dict = {}
            for k, v in dictionary.items():
                if k == 'location':
                    try:
                        foo_dict.update({'latitude': v['latitude']})
                        foo_dict.update({'longitude': v['longitude']})
                    except AttributeError:
                        continue
                elif k == 'IpAddress':
                    foo_dict.update({k: v})
                elif k == 'postal':
                    foo_dict.update({k: v['code']})
                elif isinstance(v, dict):
                    foo_dict.update({k: v['names']['en']})
                    new_data.append(foo_dict)

        print("-"*50)
        print("Last record:")
        print(new_data[-1])
        print("-"*50)
        return new_data

    def _create_db_columns_def_(self):
        """
        Creates column definitions to create new table
        :return:
        """

        columns = {}
        first_dict = self.new_data[0]

        for key, value in first_dict.items():
            columns.update({key: None})

        for key, value in first_dict.items():
            if key == 'IpAddress':
                columns[key] = 'TEXT PRIMARY KEY'
            elif isinstance(value, str):
                columns[key] = "TEXT"
            elif isinstance(value, float):
                columns[key] = "REAL"
            else:
                columns[key] = "TEXT"

        return columns

    def create_table(self):
        """
        Creates a new table in the database to sync IP geolocation data to.
        """

        col = ', '.join("'{}' {}".format(key, val) for key, val in self.columns.items())

        print("Creating GeoIP a table if one doesn't exist yet.")

        self.db.execute('''CREATE TABLE IF NOT EXISTS GeoIP
                        ({})'''.format(col))

    def save_location_data(self):
        """
        Save location data to local database
        """

        try:
            col = list(self.new_data[0].keys())
            table_col = ', '.join("'{}'".format(key) for key in col)
            col_count = len(col)
            print("Adding data to {} columns.".format(col_count))

            sql_data = []
            for d in self.new_data:
                if len(list(d.values())) == col_count:
                    sql_data.append(list(d.values()))

            def insert_data(x=1):
                """
                Local function that allows a wait period if database file is busy, then retries
                """
                try:
                    self.db.executemany("""INSERT OR REPLACE INTO GeoIP {} VALUES ({})""".format(
                         tuple(col), ",".join("?" * col_count)), sql_data)
                except AttributeError:
                    print('ERROR: You must create a table before loading to it. Try initiate_table().')
                except sqlite3.OperationalError as e:
                    if x == 5:
                        print("Renaming GeoIP to GeoIP_old and creating new table to continue sync.")
                        self.db.execute("""ALTER TABLE GeoIP RENAME TO GeoIP_old;""")

                        n_col = ', '.join("'{}' {}".format(key, val) for key, val in self.columns.items())

                        self.db.execute('''CREATE TABLE IF NOT EXISTS GeoIP
                                                    ({})'''.format(n_col))
                        insert_data()
                    else:
                        print("ERROR: {}\n Waiting 15 seconds then trying again.\nTry {} out of 5".format(e, x))
                        time.sleep(15)
                        insert_data(x + 1)

            insert_data()

            print("Table has been populated, commit to finalize operation.")

        except (AttributeError, TypeError):
            print("ERROR: You must use get_initial_data() or get_sync_data() "
                  "to grab data from Eloqua before writing to a database.")
            exit()

    def commit_and_close(self):
        """
        Commit all changes to the database
        """
        self.db.commit()
        self.db.close()
        self.reader.close()
        print("Data has been committed.")


def export_geoip(**kwargs):
    """
    Exports all tables from the SQL database, use after full IpLoc process has complete
    :param tables: List of tables to pull IP addresses from
    :param filename: File to check for tables with IP addresses
    """
    tables = kwargs.get('tables', tables_with_ip)
    filename = kwargs.get('filename', 'EloquaDB.db')

    db = sqlite3.connect(filename, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)

    for table in tables:
        print("Exporting {} georeferenced activity records from {}.".format(table, filename))
        c = db.cursor()
        sql_data = c.execute("""SELECT * FROM {} INNER JOIN GeoIP ON GeoIP.IpAddress = {}.IpAddress"""
                             .format(table, table))
        column_names = [description[0] for description in sql_data.description]
        csv_data = sql_data.fetchall()

        print("-"*50)
        print("Last record:")
        print(csv_data[-1])
        print("-"*50)
        print("Exporting {} GeoIP data to CSV.".format(table))

        with open('{} GeoIP.csv'.format(table), 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile, delimiter='|', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(column_names)
            for d in csv_data:
                writer.writerow(d)

        print("Finished exporting {}.".format(table))

    db.close()


def main():
    """
    Main function runs when file is run as main.
    """

    # Iterates through all tables with IP addresses and logs the IP with
    # its geolocation in the GeoIP table
    for tb in tables_with_ip:

        db = IpLoc(tablename=tb)
        db.ip_data()
        db.process_step()
        db.create_table()
        db.save_location_data()
        db.commit_and_close()

    # Exports GeoIP table inner joined with tables that contain activities
    # with IP addresses in csv format
    export_geoip()


# if this module is run as main it will execute the main routine
if __name__ == '__main__':
    main()
