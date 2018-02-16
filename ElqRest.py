#!/usr/bin/python
# ElqRest functions by Greg Bernard

import datetime
import requests
import config
import sqlite3
import time


API_VERSION = '2.0'  # Change to use a different API version
POST_HEADERS = {'Content-Type': 'application/json'}


class ElqRest(object):

    def __init__(self, company=config.company, username=config.username,
                 password=config.password, filename='EloquaDB.db'):
        """
        :param string username: Eloqua username
        :param string password: Eloqua password
        :param string company: Eloqua company instance
        """
        url = 'https://login.eloqua.com/id'
        req = requests.get(url, auth=(company + '\\' + username,
                                      password))

        self.filename = filename

        print("-"*50)
        print("Beginning External Activity Sync.")

        if all(arg is not None for arg in (username, password, company)):

            if req.json() == 'Not authenticated.':
                raise ValueError('Invalid login credentials')
            else:
                self.username = username
                self.password = password
                self.company = company
                self.auth = (company + '\\' + username, password)
                self.user_id = req.json()['user']['id']
                self.user_display = req.json()['user']['displayName']
                self.url_base = req.json()['urls']['base']
                self.site_id = req.json()['site']['id']

                self.rest_bs_un = req.json()['urls']['apis'][
                    'rest']['standard']
                self.rest_base = self.rest_bs_un.format(
                    version=API_VERSION)

        else:
            raise Exception(
                'Please enter all required login details: company, username, password')

    def get(self, asset_id):
        """
        Pulls external activity linked to input activity id
        :param asset_id: asset ID to pull
        :return: activity data, or none if error
        """

        url = self.rest_base + 'data/activity/' + str(asset_id)
        req = requests.get(url, auth=self.auth)

        if req.status_code == 200:
            return req.json()
        else:
            print("Error Code: {}".format(req.status_code))
            return None

    def get_activities(self, start=1, end=999999):
        """
        Use the get method to pull all available records in the provided range
        :param start: starting record ID
        :param end:  ending record ID
        :return: list of dicts containing activities data
        """
        activities = []

        for i in range(start, end):
            if self.get(i) is not None:
                activities.append(self.get(i))
            else:
                print("No more activity data, last record exported: {}.".format(i-1))
                break

        return activities

    def populate_table(self, table='External_Activity', start=None, end=99999):
        """
        Populates table in the database provided
        :param table: name of the table to create, or search in the database
        :param start: record to start from
        :param end: integer, non-inclusive
        """

        column_def = {
            'type': 'TEXT',
            'id': 'INTEGER PRIMARY KEY',
            'depth': 'TEXT',
            'name': 'TEXT',
            'activityDate': 'DATETIME',
            'activityType': 'TEXT',
            'assetName': 'TEXT',
            'assetType': 'TEXT',
            'campaignId': 'INTEGER',
            'contactId': 'INTEGER'
        }

        col = ', '.join("'{}' {}".format(key, val) for key, val in column_def.items())
        col = col + ", FOREIGN KEY(ContactId) REFERENCES contacts(ContactId)"

        db = sqlite3.connect(self.filename, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        c = db.cursor()

        try:
            c.execute("""SELECT {id} FROM {table} ORDER BY {id} DESC LIMIT 1;"""
                      .format(id='id', table=table))
        except sqlite3.OperationalError:
            c.execute('''CREATE TABLE IF NOT EXISTS {} ({})'''.format(table, col))

        # If a start value is given, starts from that, otherwise starts from the first value in the table
        # and if there is no table, starts from the first value, and continues until none are left

        try:
            if start is None:
                start = c.fetchone()[0]
            else:
                start = start

            if end != 99999:
                print("Extracting from {} to {}.".format(start, end-1))
            else:
                print("Extracting everything after: {}".format(start) + "\nThis could take a while.")

            new_data = self.get_activities(start=int(start), end=end)

        except TypeError:
            print("There is no pre-existing data in this table.")
            if end != 99999:
                print("Extracting from {} to {}.".format(1, end-1))
            else:
                print("Extracting everything... This may take a while.")
            new_data = self.get_activities(start=1, end=end)

        col_count = len(list(new_data[0].keys()))

        print("This table contains {} columns.".format(col_count))

        sql_data = []
        for d in new_data:
            # Convert unix timestamps to datetime
            d['activityDate'] = datetime.datetime.fromtimestamp(
                int(d['activityDate'])).strftime('%Y-%m-%d %H:%M:%S')
            d['id'] = int(d['id'])
            sql_data.append(list(d.values()))

        def insert_data():
            """
            Local function that allows a wait period if database file is busy, then retries
            """
            try:
                c.executemany("""INSERT OR REPLACE INTO {} VALUES ({})""".format(
                    table, ",".join("?" * col_count)), sql_data)
            except sqlite3.OperationalError:
                print("ElqRest: Another application is currently using the database,"
                      " waiting 15 seconds then attempting to continue.")
                time.sleep(15)
                insert_data()

        insert_data()

        db.commit()
        db.close()
        print("Data has been committed.")


def main():

    db = ElqRest()
    db.populate_table()


if __name__ == '__main__':
    main()
