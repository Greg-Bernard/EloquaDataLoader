import sqlite3
from geolite2 import geolite2

tables_with_ip = ["EmailClickthrough", "EmailOpen", "PageView", "WebVisit"]


class IpLoc:

    def __init__(self, **kwargs):

        self.tablename = kwargs.get("tablename", "EmailClickthrough")
        self.filename = kwargs.get("filename", 'EloquaDB.db')

        self.db = sqlite3.connect(self.filename, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        self.db.row_factory = sqlite3.Row

        self.geo_data = []
        self.new_data = []

        c = self.db.cursor()

        try:
            sql_data = c.execute('SELECT IpAddress FROM {}'.format(self.tablename))
            self.raw_ip_data = sql_data.fetchall()
        except sqlite3.OperationalError:
            print("ERROR: There is no IpAddress column in this table.")
            exit()

    def ip_data(self):
        """
        Get available location information for provided IP Addresses in SQlite Row format
        """
        reader = geolite2.reader()

        for ip in self.raw_ip_data:
            try:
                d = reader.get(ip["IpAddress"])
                d['IpAddress'] = ip["IpAddress"]
                self.geo_data.append(d)
            except ValueError:
                continue
            except TypeError:
                continue

        # print(self.geo_data[0])
        return self.geo_data

    def process_step(self):
        """
        Steps to process the raw data output from ip_data and make it suitable for analysis
        """

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
            self.new_data.append(foo_dict)

        print(self.new_data[0])
        return self.new_data

    def create_table(self):
        """
        Creates a new table in the database to sync IP geolocation data to.
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

        col = ', '.join("'{}' {}".format(key, val) for key, val in columns.items())

        print(col)

        self.db.execute('''CREATE TABLE IF NOT EXISTS GeoIP
                        ({})'''.format(col))

    def save_location_data(self):
        """
        Save location data to local database
        """
        print("-" * 50)
        print('Processing data for SQL database...')

        try:
            col = list(self.new_data[0].keys())
            col_count = len(col)
            print("Adding data to {} columns.".format(col_count))

            sql_data = []
            for d in self.new_data:
                if len(list(d.values())) == 8:
                    sql_data.append(list(d.values()))

            try:
                self.db.executemany("""INSERT OR REPLACE INTO GeoIP VALUES ({})""".format(
                    ", ".join("?" * col_count)), sql_data)
            except AttributeError:
                print('ERROR: You must create columns in the table before loading to it. Try create_columns().')
            except sqlite3.OperationalError:
                print("""ERROR: The database is locked by another program,
                please commit and close before running this script.""")
                exit()

            print("Table has been populated, commit to finalize operation.")

        except (AttributeError, TypeError):
            print('ERROR: You must use get_initial_data() or get_sync_data() '
                  'to grab data from Eloqua before writing to a database.')
            exit()

    def commit_and_close(self):
        """
        Commit all changes to the database
        """
        self.db.commit()
        self.db.close()
        geolite2.close()
        print("Data has been committed.")


def main():
    """
    Main function runs when file is run as main.
    """

    for tb in tables_with_ip:

        db = IpLoc(tablename=tb)
        db.ip_data()
        db.process_step()
        db.create_table()
        db.save_location_data()
        db.commit_and_close()


# if this module is run as main it will execute the main routine
if __name__ == "__main__":
    main()