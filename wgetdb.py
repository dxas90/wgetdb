#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""wgetdb

Usage:
  wgetdb <database_path> <url> [<label>]
  wgetdb -h | --help
  wgetdb --version

Options:
  -h --help     Show this screen.
  --version     Show version.
"""

import datetime
from urllib import request
from sqlite3 import dbapi2 as sqlite3
from time import sleep

from docopt import docopt
import hashlib


__version__ = "0.1.4"
__author__ = "Daniel Ramirez"
__license__ = "MIT"

URLOPEN_TIMEOUT = 10
TABLE_NAME = "datas"
CREATE_TABLE_SQL = """
    CREATE TABLE %s (
        id INTEGER PRIMARY KEY,
        url VARCHAR(4095) NOT NULL,
        label VARCHAR(255) NOT NULL,
        data BLOB NOT NULL,
        created_date DATE NOT NULL,
        modified_date DATE NOT NULL,
        UNIQUE(url, label)
    );
""" % TABLE_NAME


def md5sum(t):
    return hashlib.md5(t).hexdigest()


class UrlDB(object):
    def __init__(self, db_path, wait_before=0):
        self.db_path = db_path
        self.wait_before = wait_before
        self._con = None

    def __del__(self):
        self.con.close()

    @property
    def con(self):
        if not self._con:
            self._con = sqlite3.connect(self.db_path, isolation_level=None)
            self._con.row_factory = sqlite3.Row
            self.create_table()
        return self._con

    def create_table(self):
        cur = self.con.execute(
            "SELECT * FROM sqlite_master WHERE type='table' AND name=?",
            (TABLE_NAME,))
        if cur.fetchone() is None:
            self.con.execute(CREATE_TABLE_SQL)

    def download_url(self, url):
        if self.wait_before:
            sleep(self.wait_before)
        response = request.urlopen(url, timeout=URLOPEN_TIMEOUT)
        if response.code != 200:
            return None
        return response.read()

    def insert_data(self, url, label, data):
        sql = ('INSERT INTO %s ("url", "label", "data", "created_date", "modified_date")'
               'VALUES (?, ?, ?, ?, ?);') % TABLE_NAME
        args = (url, label, data, datetime.datetime.utcnow(),
                datetime.datetime.utcnow())
        self.con.execute(sql, args)

    def update_data(self, url, label, data):
        sql = ('UPDATE %s SET "data" = ?, "modified_date" = ?'
               'WHERE "url" = ? AND "label" = ?;') % TABLE_NAME
        args = (data, datetime.datetime.utcnow(), url, label)
        self.con.execute(sql, args)

    def store(self, url, label):
        data = self.download_url(url)
        try:
            self.insert_data(url, label, data.strip())
        except sqlite3.IntegrityError:
            self.update_data(url, label, data)

    def get(self, url, label):
        sql = 'SELECT * FROM %s WHERE "url" = ? AND "label" = ?;' % TABLE_NAME
        args = (url, label)
        cur = self.con.execute(sql, args)
        record = list(cur)
        if len(record) > 0:
            record = record[0]
            return {'data': str(record[3]),
                    'created_date': record[4],
                    'modified_date': record[5]}
        return None


def main():
    try:
        args = docopt(__doc__, version=__version__)
        db_path = args.get('<database_path>')
        url = args.get('<url>')
        label = args.get('<label>') if args.get('<label>') else md5sum(url.encode('utf-8'))
        store_url = UrlDB(db_path)
        store_url.store(url, label)
        print('SUCCESS!')
    except Exception as e:
        print(u'=== ERROR ===')
        print(u'type:{0}'.format(type(e)))
        print(u'args:{0}'.format(e.args))
        print(u'message:{0}'.format(e))


if __name__ == '__main__':
    main()
