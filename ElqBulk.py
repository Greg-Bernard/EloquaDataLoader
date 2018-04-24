#!/usr/bin/python
# ElqBulk by Greg Bernard
# Credits to Jeremiah Coleman's fantastic package pyeloqua:
# https://pypi.python.org/pypi/pyeloqua/0.5.10

import sqlite3
from json import dump
from pyeloqua import Bulk, Eloqua
import config
import TableNames
import time

__version__ = '0.1.0'


# List of available tables in Eloqua, anything outside of this list will return errors.
# Support for campaignResponses may come at a later date.

# ElqBulk takes the inputs filename, table, and columns
# filename = string
# table = string (Must match the name of the activity that you wish to export)
# It also accepts login credentials, in-case you don't want to set up a config file
# company = string (Eloqua Company Name)
# username = string (Eloqua Username)
# password = string (Eloqua Password)


class ElqBulk(object):
    """
    A class to pull, process, and store all relevant information from Eloqua's BULK 2.0 API
    """

    def __init__(self, **kwargs):

        # By default ElqBulk will be set up to pull contacts when initialised.
        self.filename = kwargs.get('filename', 'EloquaDB.db')
        self.table = kwargs.get('table', 'contacts')
        self.company = kwargs.get('company', config.company)
        self.username = kwargs.get('username', config.username)
        self.password = kwargs.get('password', config.password)
        self.data = kwargs.get('data', None)

        if self.table not in TableNames.tables:
            raise ValueError("Input table name is not within the list of accepted parameters.")

        self.bulk = self._initialize_bulk_()
        # self.rest = self._initialize_elq_()

        # _create_DB_columns_def fills self.columns with all necessary column information
        self.columns = self._create_db_columns_def_()

        self.db = sqlite3.connect(self.filename, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        self.db.row_factory = sqlite3.Row

    def _initialize_bulk_(self):
        """
        Initialize Bulk class
        """

        bulk = Bulk(username=self.username, password=self.password, company=self.company)
        print("-" * 50)
        print("Initialized connection to Eloqua")
        print("Beginning {} sync.".format(self.table))
        print("-" * 50)

        # Cover all 3 possible inputs for self.table
        # convert input into appropriate export type for Eloqua

        if self.table == 'contacts':
            exp_type = 'contacts'
        elif self.table == 'accounts':
            exp_type = 'accounts'
        else:
            exp_type = 'activities'

        # Cover all possible inputs for self.tabe
        # clears activity type if export is contacts or accounts

        if self.table == 'contacts':
            act_type = None
        elif self.table == 'accounts':
            act_type = None
        else:
            act_type = self.table

        # specify that we want an export of activity records:
        bulk.exports(exp_type, act_type=act_type)

        return bulk

    def _initialize_elq_(self):
        """
        Initialize Eloqua object
        :return:
        """

        elq = Eloqua(username=self.username, password=self.password, company=self.company)
        elq.GetAsset(assetType='activity', assetId=None)
        return elq

    def _create_db_columns_def_(self):
        """
        Create a dictionary of column definitions from the list of fields returned by Eloqua
        """

        print("Loading list of available columns to create table...")
        fields = self.bulk.get_fields()  # This will give us a list of the available fields and their names
        # print("@"*50)
        # print(fields)

        # Extract values from nested dictionaries with key = 'internalName' or 'name'
        # and append it into the list 'column'

        column = {}
        for d in fields:
            try:
                column.update({d['internalName']: d['dataType']})
            except KeyError:
                try:
                    column.update({d['internalName']: 'TEXT'})
                except KeyError:
                    try:
                        column.update({d['name']: 'dataType'})
                    except KeyError:
                        column.update({d['name']: 'TEXT'})

        for key, value in column.items():
            if key == 'ActivityId':
                column[key] = 'TEXT PRIMARY KEY'
            elif key == 'contactID':
                column[key] = 'TEXT PRIMARY KEY'
            elif key == 'accountID':
                column[key] = 'TEXT PRIMARY KEY'
            elif key == 'createdAt':
                column[key] = "TIMESTAMP"
            elif key == 'updatedAt':
                column[key] = "TIMESTAMP"
            elif key == 'ActivityDate':
                column[key] = "TIMESTAMP"
            elif value == 'string':
                column[key] = 'TEXT'
            elif value == 'date':
                column[key] = 'DATE'
            elif value == 'number':
                column[key] = 'INTEGER'

            else:
                column[key] = 'TEXT'

        return column

    # ------------------------------------------------------------------------------------------
    # Helper Methods
    # ------------------------------------------------------------------------------------------

    def create_table(self):
        """
        Creates a SQL database file if one does not yet exist, and a table if one does not
        yet exist to dump active data into.
        Also initiates row_factory to allow ElsDB to write to the table
        """

        col = ', '.join("'{}' {}".format(key, val) for key, val in self.columns.items())

        self.db.execute('''CREATE TABLE IF NOT EXISTS {}
                            ({})'''.format(self.table, col))

    def get_initial_data(self):
        """
        PyEloqua initial data pull
        """

        # -----------------------------------------------------------
        # CREATING DEFINITION FOR EXPORT FROM ELOQUA

        print("Loading list of available data...")
        fields = self.bulk.get_fields()  # This will give us a list of the available fields and their names

        # Extract values from nested dictionaries with key = 'name'
        # and append it into the list 'fieldlist'
        _col = []
        for record in fields:
            _col.append(record['name'])

        print("\n {} \n".format(_col))

        # Add the fields dump as the export list to get all fields
        self.bulk.add_fields(_col)

        # END FIELD DEFINITION
        # -----------------------------------------------------------

        print("Sending Export definition to Eloqua...")
        self.bulk.create_def('Bulk Export - {}'.format(self.table))  # send export info to Eloqua

        print("Loading Eloqua data to instance. This may take a while...")
        self.bulk.sync()

        # Now export individual rows
        self.data = self.bulk.get_export_data()

        # Print a divider line and the first record of data
        print("\n" + '#' * 50 + "\n")
        print("First record in export:" + "\n")
        print(self.data[0])

        _count = self.bulk.get_export_count()
        print("\n" + '#' * 50 + "\n")
        print("Count of {} records in Eloqua: {}".format(self.table, _count))

        print("Finished loading {} activity data.".format(self.table))

        return self.data

    def get_sync_data(self):
        """
        PyEloqua initial data pull
        Will always retrieve at least 1 record.
        """

        # -----------------------------------------------------------
        # CREATING DEFINITION FOR EXPORT FROM ELOQUA

        print("Loading list of available data...")
        fields = self.bulk.get_fields()  # This will give us a list of the available fields and their names

        # Extract values from nested dictionaries with key = 'name'
        # and append it into the list 'fieldlist'
        _col = []
        for record in fields:
            _col.append(record['name'])

        print("\n {} \n".format(_col))

        # Add the fields dump as the export list to get all fields
        self.bulk.add_fields(_col)

        # Section to filter data pulled from eloqua to new information only

        if self.table == "contacts":
            date_field = "updatedAt"
        elif self.table == "accounts":
            date_field = "updatedAt"
        else:
            date_field = "ActivityDate"

        # Find the last date in updatedAt or ActivityDate
        try:
            c = self.db.cursor()

            try:
                c.execute(
                    """SELECT {} AS "{} [timestamp]" FROM {} ORDER BY {} DESC LIMIT 1;""".format(
                        date_field, date_field, self.table, date_field))
            except sqlite3.OperationalError:
                print("ERROR: You must create a table before you can sync to it.\nTry create_table().")

            try:
                max_update = c.fetchone()[0]
                self.bulk.filter_date(field=date_field, start=max_update)
                print("Extracting everything after: {}".format(max_update))
            except TypeError:
                print("There is no pre-existing data in this table.")

        except AttributeError:
            print("ERROR: You must initialize your SQL connection before"
                  " you can sync to it.\nUse initiate_sql() first, or get"
                  "_initial_data() if you just want to pull data to work with.")
            exit()

        # Grab the latest date from the SQLite Database for the specified table

        # Add filter to selected date column by last date in system

        # END FIELD DEFINITION
        # -----------------------------------------------------------

        print("Sending Export definition to Eloqua...")
        self.bulk.create_def('Bulk Export - {}'.format(self.table))  # send export definition to Eloqua

        print("Finished loading {} activity data.".format(self.table))

        print("Loading Eloqua data to instance. This may take a while...")
        self.bulk.sync()

        # Now export individual rows
        self.data = self.bulk.get_export_data()

        # Print a divider line and the first record of data
        print("\n" + '#' * 50 + "\n")
        print("First record in export:" + "\n")
        print(self.data[0])

        _count = self.bulk.get_export_count()
        print("\n" + '#' * 50 + "\n")
        print("Count of new {} records in Eloqua: {}".format(self.table, _count))

        return self.data

    def dump_to_json(self):
        """
        Dump current export data into a json file
        """
        try:
            with open('bulk_export_{}.json'.format(self.table), 'w') as fopen:
                dump(self.data, fopen, indent=3)
        except AttributeError:
            print('ERROR: You must use get_initial_data() or get_sync_data() '
                  'to grab data from Eloqua before dumping to a file.')
            exit()

    def load_to_database(self):
        """
        Load contacts to appropriate database table
        """

        print("-" * 50)
        print('Processing data for SQL database...')

        try:
            col = list(self.data[0].keys())

            col_count = len(col)

            print("This table contains {} columns.".format(col_count))

            sql_data = []
            for d in self.data:
                sql_data.append(list(d.values()))

            # Initialize database connection, if database is locked, waits 1 minute, then retries

            def insert_data(x=1):
                """
                Local function that allows a wait period if database file is busy, then retries
                """
                try:                  
                    self.db.executemany("""INSERT OR REPLACE INTO {} {} VALUES ({})""".format(
                        self.table, tuple(col), ",".join("?" * col_count)), sql_data)
                except AttributeError:
                    print('ERROR: You must create a table before loading to it. Try initiate_table().')
                except sqlite3.OperationalError as e:
                    if x == 5:
                        print("Renaming {t} to {t}_old and creating new table to continue sync.".format(t=self.table))
                        self.db.execute("""ALTER TABLE {tname} RENAME TO {tname}_old;""".format(tname=self.table, ))
                        n_col = ', '.join("'{}' {}".format(key, val) for key, val in self.columns.items())

                        self.db.execute('''CREATE TABLE IF NOT EXISTS {}
                                                    ({})'''.format(self.table, n_col))
                        insert_data()
                    else:
                        print("ERROR: {}\n Waiting 15 seconds then trying again.\nTry {} out of 5".format(e, x))
                        time.sleep(15)
                        insert_data(x + 1)

            insert_data()

            print("Table has been populated, commit() to finalize operation.")

        except (AttributeError, TypeError):
            print('ERROR: You must use get_initial_data() or get_sync_data() '
                  'to grab data from Eloqua before writing to a database.')
            exit()

    def commit(self):
        """
        Commit all changes to teh database
        """
        self.db.commit()
        print("Data has been committed, close() when finished")

    def clear(self):
        """
        Clears out the database by dropping the current table
        """
        self.db.execute('DROP TABLE IF EXISTS {}'.format(self.table))

    def close(self):
        """
        Safely close down the database
        """
        self.db.close()
        print('Database has been safely closed.')


def main():
    """
    create db for testing
    table must be 'contacts', 'accounts', or your activity type according to
    the system name in Eloqua. See system_fields for more information
    """

    # tb = ElqBulk(filename='fest.db', table='EmailClickthrough')
    # tb.create_table()
    # # tb.get_initial_data()
    # tb.get_sync_data()
    # tb.load_to_database()
    # tb.commit()
    # tb.close()


# if this module is run as main it will execute the main routine
if __name__ == "__main__":
    main()
