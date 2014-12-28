from bs4 import BeautifulSoup
import urllib2 as u2
import pprint
from Queue import Queue
import re
import time
import threading
import sqlite3
import html2text
import os
import twitter
from config import *
import datetime
import logging
 
logging.basicConfig(filename='log.txt',level=logging.DEBUG)

class SetQueue(Queue):
    ''' Subclass the Queue to keep track of all items that have
    entered the queue to keep from adding duplicates
    '''

    def __init__(self, maxsize=0):
        Queue.__init__(self, maxsize)
        self.all_items = set([])

    def put(self, item):
        if item not in self.all_items:
            Queue.put(self, item)
            self.all_items.add(item)

    def get(self):
        item =  Queue.get(self)
        return item

class Thready(threading.Thread):

    def __init__(self, queue, tweet_queue):
        threading.Thread.__init__(self)
        self.queue = queue
        self.twitter = Twitter(tweet_queue, auth=False)

    def run(self):
        ''' A new thread isn't technically created until run() is called.
        This causes SQLite to throw a fit when connections are open in multiple threads
        '''
        self.conn_obj = Connection()
        while True:
            url = self.queue.get()
            try:
                html = u2.urlopen(url).read().decode('utf-8')
            except Exception as e:
                # unhandled exceptions are bad with threads.
                logging.warning("Error querying %s: %s" % (url, e))
            else:
                # retrieve Page Text and Position Title
                page_data = html2text.html2text(html)
                url_soup = BeautifulSoup(html)
                link = Link(url, url_soup)
                
                if link.report_type is not None:
                    if self.conn_obj.count_urls(url) == 0:
                        logging.info('New %s URL found: %s' % (link.report_type, url))
                        self.conn_obj.add_url(url, page_data) # add to database
                        self.twitter.queue_new(link.report_type, link.title, link.url) # queue tweet
                    else:
                        logging.info('Existing %s URL found: %s' % (link.report_type, url))

                for each_link in Link.linked_links(url_soup):
                    formatted_link = Link.canonicalize(each_link.attrs['href'])
                    if formatted_link is not None and Link.is_whitelisted(formatted_link):
                        self.queue.put(formatted_link)
            self.queue.task_done()

class Link():
    def __init__(self, url, soup):
        self.url = url
        self.soup = soup
        self.report_type = self.get_type()
        self.title = self.get_title(soup)

    @staticmethod
    def linked_links(soup):
        return soup.find_all('a', href=True)

    @staticmethod
    def canonicalize(url):
        if url is None or len(url)==0:
            return None
        if url[0] == '/': # relative url
            url = 'spacex.com' + url
        # remove mixed types and appended forwardslashes
        url = url.replace('http://','').replace('www.','').strip('/') 
        return 'http://' + url

    @staticmethod
    def is_whitelisted(url):
        if not Link.is_internal(url) or 'download' in url:
            return False
        return True

    @staticmethod
    def is_internal(url):
        if '://spacex.com' not in url:
            return False
        return True

    def get_type(self):
        if '/careers/position/' in self.url:
            return 'job'
        elif re.search(r'/news/\d{4}/\d{2}/\d{2}', self.url) is not None:
            return 'news'
        elif re.search(r'/press/\d{4}/\d{2}/\d{2}', self.url) is not None:
            return 'press'
        elif re.search(r'/media-gallery/detail/\d{6}/\d{4}', self.url) is not None:
            return 'image'
        return None

    def get_title(self, soup):
        ''' Given a Beautiful Soup object, find the title '''
        try:
            if type == 'job':
                title = soup.find('h2', 'position-title').text
            elif type == 'news' or type == 'press':
                title = soup.find('h1', 'title').text
            elif type == 'image':
                title = soup.find(id='asset-title').text
        except:
            title = None
        else:
            title = None
        return title


class Connection():

    def __init__(self, file='db.db'):
        self.db = file
        self.conn = sqlite3.connect(self.db)

    def create_table(self):
        self.conn.cursor().execute('''CREATE TABLE IF NOT EXISTS spacex (
                                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                                     link TEXT,
                                     page TEXT)''')
    
    def count_urls(self, url=None):
        ''' If no url is specified, return the total length of total '''
        cur = self.conn.cursor()
        sql = 'SELECT COUNT(*) FROM spacex'
        if url is not None:
            sql = sql + ' WHERE link = ?'
            cur.execute(sql, (url,))
        else:
            cur.execute(sql)
        return cur.fetchone()[0]

    def add_url(self, url, page):
        sql = 'INSERT INTO spacex (link, page) values (?, ?)'
        cur = self.conn.cursor()
        cur.execute(sql, (url, page))
        self.conn.commit()

class Twitter(twitter.Twitter):

    def __init__(self, queue, auth=True):
        self.queue = queue
        if auth == True:
            twitter.Twitter.__init__(self, auth=twitter.OAuth(
                                        twitter_access_token,
                                        twitter_access_secret,
                                        twitter_consumer_key,
                                        twitter_consumer_secret))

    def queue_new(self, type, title, url):
        if type == 'press':
            str = 'press release'
        elif type == 'news':
            str = 'article'
        elif type == 'job':
            str = 'job'
        elif type == 'image':
            str = 'image'
        else:
            str = 'link'
        if title is not None:
            tweet = 'New %s: %s %s' % (str, title, url)
        else:
            tweet = 'New %s: %s' % (str, url)
        self.queue.put(tweet)

    def tweet(self, msg):
        self.statuses.update(status=msg)

   

if __name__ == '__main__':
    logging.info('SpaceXNews.py is starting at %s' % datetime.datetime.now())
    conn_obj = Connection()
    conn_obj.create_table()
    should_tweet = (conn_obj.count_urls() != 0)
    logging.info('Should Tweet during this job?...%s' % should_tweet)
    conn_obj.conn.close()
    queue = SetQueue()
    queue.put('http://www.spacex.com')
    tweet_queue = SetQueue()
    num_workers = 5 
    for i in range(num_workers):
        t = Thready(queue, tweet_queue)
        t.setDaemon(True)
        t.start()
    queue.join() # wait for threads to finish
    logging.info('Finished searching.')
    if should_tweet:
        twit = Twitter(tweet_queue, auth=True)
        while not twit.queue.empty():
            time.sleep(1)
            try:
                msg = twit.queue.get()
                logging.info("Tweeting: %s" % msg)
                twit.tweet(msg)
            except Exception as e:
                logging.error('Error sending tweet: %s' % e)
