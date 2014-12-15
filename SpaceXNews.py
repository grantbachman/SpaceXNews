from bs4 import BeautifulSoup
import urllib2 as u2
import pprint
from Queue import Queue
import time
import threading
import sqlite3
import html2text
import os
import twitter
from config import *
import datetime
import logging
 
logging.basicConfig(filename='SpaceXNews.log',level=logging.DEBUG)

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
                soup = BeautifulSoup(html)
                title = get_title(soup)

                # Check if it's a position, and a new position.
                if title is not None and self.conn_obj.count_urls(url) == 0:
                    logging.info('New URL found: %s' % url)
                    self.conn_obj.add_url(url, page_data) # add to database
                    self.twitter.queue_new_position(title, url) # queue tweet

                links = soup.find_all('a', href=True)
                for link in links:
                   formatted_link = canonicalize(link.attrs['href'])
                   if is_whitelisted(formatted_link):
                       self.queue.put(formatted_link)
            self.queue.task_done()

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

    def queue_new_position(self, title, url):
        tweet = 'New position: %s. %s' % (title, url)
        self.queue.put(tweet)

    def tweet(self, msg):
        self.statuses.update(status=msg)

def is_whitelisted(url):
    if 'careers' in url and 'category' not in url:
        return True
    return False

def get_title(soup):
    try:
        title = soup.find('h2', 'position-title').text
    except:
        title = None
    finally:
        return title
    
def canonicalize(url):
    if url[0] == '/': # relative url
        url = 'spacex.com' + url
    # remove mixed types and appended forwardslashes
    url = url.replace('http://','').replace('www.','').strip('/') 
    return 'http://' + url

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
                loggin.error('Error sending tweet: %s' % e)
