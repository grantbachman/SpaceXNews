import unittest
import os
import tempfile
from SpaceXNews import *


class TestConnection(unittest.TestCase):

    def setUp(self):
        # returns a file_descriptor/absolute path tuple. 
        self.temptup = tempfile.mkstemp()
        self.conn_object = Connection(self.temptup[1])
        self.conn_object.create_table()
        self.connection = self.conn_object.conn

    def tearDown(self):
        os.close(self.temptup[0])
        os.unlink(self.temptup[1])

    def test_table_is_created(self):
        cur = self.connection.cursor()
        cur.execute("SELECT COUNT(*) from spacex")
        assert(cur.fetchone()[0] == 0)

    def test_count_urls(self):
        assert(self.conn_object.count_urls() == 0)
        cur = self.connection.cursor()
        sql = 'INSERT INTO spacex (link, page) values (?, ?)'
        url = 'http://spacex.com'
        cur.execute(sql, (url,'This is data'))
        self.connection.commit()
        assert(self.conn_object.count_urls() == 1)
        assert(self.conn_object.count_urls(url) == 1)
        assert(self.conn_object.count_urls('http://NotInTable.com') == 0)

    def test_add_url(self):
        assert(self.conn_object.count_urls() == 0)
        self.conn_object.add_url('http://spacex.com', 'test data')
        assert(self.conn_object.count_urls() == 1)
