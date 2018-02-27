#!/usr/bin/python
# ElqRest functions by Greg Bernard

import datetime
import requests
import config
import sqlite3
import time
import TableNames


API_VERSION = '2.0'  # Change to use a different API version
POST_HEADERS = {'Content-Type': 'application/json'}


class ElqRest(object):

    def __init__(self, sync=None, company=config.company, username=config.username,
                 password=config.password, filename='EloquaDB.db'):
        """
        :param string sync: Eloqua object to sync to database,
                            if you provide a value all relevant methods will automatically be called
                            current support: campaigns, campaign, external
        :param string username: Eloqua username
        :param string password: Eloqua password
        :param string company: Eloqua company instance
        :param string filename: Name of database file
        """

        url = 'https://login.eloqua.com/id'
        req = requests.get(url, auth=(company + '\\' + username,
                                      password))

        self.sync = sync
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

        self.db = sqlite3.connect(self.filename, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        self.c = self.db.cursor()

    # BASE GET METHOD ----------------------------------------------------------------------------------------

    def get(self, asset_id=None, count=500, page=1):
        """
        Get REST API 2.0 data from Eloqua
        :param asset_id: If your asset is meant to only pull one at a time,
                        provide the ID for the asset you wish to pull
        :param count: If your asset is pulled in batches, the size of the batch
        :param page: The page you wish to pull, size of the page is determined by your batch size
        :return: The requested data
        """

        asset = self.sync
        depth = ""
        multi_assets = ['campaigns', ]

        if asset == 'external':
            asset_type = 'data/activity/'
        elif asset == 'campaigns':
            asset_type = 'assets/campaigns'
            depth = 'depth=partial&'
        elif asset == 'campaign':
            asset_type = 'assets/campaign/'
        else:
            raise Exception(
                "Please enter an accepted REST input: external, campaign, campaigns")

        if count is None:
            count_item = ""
        elif (count is not None) and (asset not in multi_assets):
            # print("{} does not accept the input count, removing.".format(asset))
            count_item = ""
        else:
            count_item = "count={}&".format(count)

        if page is None:
            page_item = ""
        elif (page is not None) and (asset not in multi_assets):
            # print("{} does not accept the input page, removing.".format(asset))
            page_item = ""
        else:
            page_item = "page={}&".format(page)

        if (asset not in multi_assets) and (asset_id is not None):
            asset_id = asset_id
        else:
            asset_id = "?"

        url = self.rest_base + str(asset_type) + \
            str(asset_id) + page_item + count_item + depth
        # print(url)
        req = requests.get(url, auth=self.auth)

        if req.status_code == 200:
            return req.json()
        else:
            print("Error Code: {}".format(req.status_code))
            return None

    # GET SPECIFIC DATA FROM REST ---------------------------------------------------------------------------

    def get_activities(self, start=1, end=999999):
        """
        Use the get method to pull all available records in the provided range
        :param start: starting record ID
        :param end:  ending record ID
        :return: list of dicts containing activities data
        """
        activities = []

        for i in range(start, end):
            if self.get(asset_id=i) is not None:
                activities.append(self.get(asset_id=i))
            else:
                print("No more activity data, last record exported: {}.".format(i-1))
                break

        self.sync = 'external'

        return activities

    def get_campaigns(self, count=100, p_start=1, p_end=999999):
        """
        Pulls all campaigns from Eloqua in a defined range.
        :param count: Size of batch to pull per page
        :param p_start: Page to start on
        :param p_end: Page to finish on
        :return:
        """
        campaigns = []

        print("Starting export on page: {}".format(p_start))

        for i in range(p_start, p_end):
            if len(self.get(count=count, page=i)['elements']) != 0:
                campaigns.extend(self.get(count=count, page=i)['elements'])
            else:
                print("No more campaign data, last page exported: {}".format(i-1))
                break

        self.sync = 'campaigns'

        return campaigns

    # DATA INSERTION  ----------------------------------------------------------------------------------------

    def insert_data(self, table, col_count, sql_data):
        """
        Local function that allows a wait period if database file is busy, then retries
        """

        try:
            self.c.executemany("""INSERT OR REPLACE INTO {} VALUES ({});""".format(
                table, ",".join("?" * col_count)), sql_data)
        except sqlite3.OperationalError:
            print("ElqRest: Another application is currently using the database,"
                  " waiting 15 seconds then attempting to continue.")
            time.sleep(15)
            self.insert_data(table, col_count, sql_data)

        self.db.commit()
        self.db.close()
        print("Data has been committed.")

    # DATA PROCESSING STEPS ----------------------------------------------------------------------------------

    def export_campaigns(self, table='Campaigns'):
        """
        Populates the campaigns table in the database.
        :param table: name of the table to create, or search in the database
        """

        col = ', '.join("'{}' {}".format(key, val) for key, val in TableNames.campaign_col_def.items())

        try:
            self.c.execute("""SELECT COUNT(*) FROM {table};""".format(table=table))
            row_count = self.c.fetchall()[0][0]
            p_start = row_count // 100
            new_data = self.get_campaigns(count=100, p_start=p_start)
        except sqlite3.OperationalError:
            self.c.execute('''CREATE TABLE IF NOT EXISTS {table} ({columns});'''
                           .format(table=table, columns=col))
            new_data = self.get_campaigns(count=100)

        sql_data = []
        date_columns = [k for k, v in TableNames.campaign_col_def.items() if v.find('DATETIME') >= 0]

        for d in new_data:
            dic = {}

            for c in date_columns:
                # Convert unix timestamps to datetime
                try:
                    d[c] = datetime.datetime.fromtimestamp(
                        int(d[c])).strftime('%Y-%m-%d %H:%M:%S')
                except KeyError:
                    d[c] = ""
                    continue

            try:
                d['Field 1'] = d['fieldValues'][0]['value']
                d['Field 2'] = d['fieldValues'][1]['value']
                d['Field 3'] = d['fieldValues'][2]['value']
            except KeyError:
                d['Field 1'] = ""
                d['Field 2'] = ""
                d['Field 3'] = ""

            for k in TableNames.campaign_col_def.keys():
                try:
                    dic[k] = d[k]
                except KeyError:
                    dic[k] = ''
                    continue

            sql_data.append(list(dic.values()))

        print("-"*50)
        col_count = len(sql_data[0])

        self.insert_data(table=table, col_count=col_count, sql_data=sql_data)

    def export_external(self, table='External_Activity', start=None, end=99999):
        """
        Populates external activity table in the database.
        :param table: name of the table to create, or search in the database
        :param start: record to start from
        :param end: integer, non-inclusive
        """

        col = ', '.join("'{}' {}".format(key, val) for key, val in TableNames.external_col_def.items())
        col = col + ", FOREIGN KEY(ContactId) REFERENCES contacts(ContactId)"

        try:
            self.c.execute("""SELECT {id} FROM {table} ORDER BY {id} DESC LIMIT 1;"""
                           .format(id='id', table=table))
        except sqlite3.OperationalError:
            self.c.execute('''CREATE TABLE IF NOT EXISTS {} ({});'''.format(table, col))

        # If a start value is given, starts from that, otherwise starts from the first value in the table
        # and if there is no table, starts from the first value, and continues until none are left

        try:
            if start is None:
                start = self.c.fetchone()[0]
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

        self.insert_data(table=table, col_count=col_count, sql_data=sql_data)


def main():

    db = ElqRest(sync='campaigns')
    db.export_campaigns()


if __name__ == '__main__':
    main()
