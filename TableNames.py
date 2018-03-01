#!/usr/bin/python

"""
All BULK exportable tables in Eloqua
DO NOT MODIFY
"""

tables = ['accounts', 'contacts', 'EmailOpen', 'EmailClickthrough', 'EmailSend', 'Subscribe', 'Unsubscribe',
          'Bounceback', 'WebVisit', 'PageView', 'FormSubmit']

campaign_col_def = {
            'currentStatus': 'TEXT',
            'id': 'INTEGER PRIMARY KEY',
            'createdAt': 'DATETIME',
            'createdBy': 'INTEGER',
            'depth': 'TEXT',
            'name': 'TEXT',
            'updatedAt': 'TIMESTAMP',
            'updatedBy': 'INTEGER',
            'actualCost': 'REAL',
            'budgetedCost': 'REAL',
            'product': 'TEXT',
            'region': 'TEXT',
            'campaignCategory': 'TEXT',
            'Field 1': 'TEXT',
            'Field 2': 'TEXT',
            'Field 3': 'TEXT',
            'firstActivation': 'DATETIME',
            'memberCount': 'INTEGER',
            'startAt': 'DATETIME',
            'endAt': 'DATETIME'
        }

external_col_def = {
            'type': 'TEXT',
            'id': 'INTEGER PRIMARY KEY',
            'depth': 'TEXT',
            'name': 'TEXT',
            'activityDate': 'TIMESTAMP',
            'activityType': 'TEXT',
            'assetName': 'TEXT',
            'assetType': 'TEXT',
            'campaignId': 'INTEGER',
            'contactId': 'INTEGER'
        }

users_col_def = {'type': 'TEXT',
                 'id': 'INTEGER PRIMARY KEY',
                 'createdAt': 'DATETIME',
                 'createdBy': 'INTEGER',
                 'depth': 'TEXT',
                 'description': 'TEXT',
                 'name': 'TEXT',
                 'updatedAt': 'TIMESTAMP',
                 'updatedBy': 'INTEGER',
                 'company': 'TEXT',
                 'emailAddress': 'TEXT',
                 'loginName': 'TEXT'
                 }
