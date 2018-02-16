#!/usr/bin/python
# haversine distances to major population centers by Greg Bernard

import numpy as np
import pandas as pd
import sqlite3
import re


class CityAppend:

    def __init__(self, filename='EloquaDB.db', table='GeoIP'):

        self.filename = filename
        self.table = table
        self.db = sqlite3.connect(self.filename, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        try:
            self.cities = pd.read_pickle("converted_city_data.p")
        except FileNotFoundError:
            self.cities = self.pull_cities()
        self.data = self.pull_data()

    def pull_cities(self):
        """
        Pulls city longitude and latitude info from Wikipedia
        :return: data frame with city information
        """

        countries = ['Canada', 'United States']
        list_of_locations = 'https://en.wikipedia.org/wiki/' \
                            'List_of_population_centers_by_latitude'

        self.cities = pd.read_html(list_of_locations)
        self.cities[0].dropna(inplace=True)
        self.cities = self.cities[0].loc[self.cities[0][4].isin(countries)]

        def convert(coord):
            coord_list = re.split("\W+", coord)
            print(coord_list)
            new_coord = (float(coord_list[0]) + (float(coord_list[1]) / 60)) * \
                        (-1 if ('S' in coord_list[2] or 'W' in coord_list[2]) else 1)
            return new_coord

        self.cities['Lat'] = self.cities.apply(lambda row: convert(row[0]), axis=1)
        self.cities['Lon'] = self.cities.apply(lambda row: convert(row[1]), axis=1)

        self.cities.to_pickle("converted_city_data.p")

        return self.cities

    def pull_data(self):
        """
        Pulls current GeoIP data from the database
        :return: data frame with data
        """

        sql_data = pd.read_sql("""SELECT * FROM GeoIP;""", con=self.db)

        return sql_data

    def haversine(self):
        """
        Distance between two sets of coordinates in kilometers (5% inaccurate)
        :return: list of closest cities, country those cities are in, distance in kilometers
        """
        radius = 6371  # radius of Earth in KM

        try:
            lat = np.radians(self.data.latitude)
            lon = np.radians(self.data.longitude)
        except AttributeError:
            lat = np.radians(self.data[3])
            lon = np.radians(self.data[4])

        end_lon = np.radians(self.cities.Lon)
        end_lat = np.radians(self.cities.Lat)

        city = self.cities[2]
        country = self.cities[4]

        min_distances = []
        min_cities = []
        min_countries = []

        # Return ID of row with minimum value, then pick the row with that ID from city and country
        for row in zip(lat, lon):
            x = (end_lon - row[1]) * np.cos(0.5 * (end_lat + row[0]))
            y = end_lat - row[0]
            distance = radius * np.sqrt(x ** 2 + y ** 2)
            row_value = distance.idxmin()
            min_distances.append(distance.loc[row_value])
            min_cities.append(city.loc[row_value])
            min_countries.append(country.loc[row_value])

        return min_cities, min_countries, min_distances

    def closest_cities(self):
        """
        Calculate the closest major population center for every possible marketing activity
        :return: updated data data frame
        """

        print("-"*50)
        print("Calculating closest city for each IP.")
        self.data['cc_city'], self.data['cc_country'], self.data['cc_distance_in_km'] = self.haversine()

        return self.data

    def load_to_database(self):
        """
        Load data back into database
        """

        data_types = {'city': 'TEXT',
                      'country': 'TEXT',
                      'latitude': 'REAL',
                      'longitude': 'REAL',
                      'postal': 'TEXT',
                      'registered_country': 'TEXT',
                      'IpAddress': 'TEXT PRIMARY KEY',
                      'cc_city': 'TEXT',
                      'cc_country': 'TEXT',
                      'cc_distance_in_km': 'REAL',
                      }

        print("Loading to database.")
        self.data.to_sql(self.table, con=self.db, if_exists='replace', index=False, dtype=data_types)
        self.db.commit()
        self.db.close()


def main():

    ca = CityAppend()
    print(ca.closest_cities())
    ca.load_to_database()
    # closest_cities().to_csv("closest_city.csv", sep=',', header=True, index=False)


if __name__ == "__main__":
    main()
