from bs4 import BeautifulSoup
import urllib2 as u2
import pprint
from Queue import Queue
import re
import time
import threading
import sqlite3
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
            thread_name = threading.current_thread().name
            try:
                html = u2.urlopen(url).read().decode('utf-8')
            except Exception as e:
                # unhandled exceptions are bad
                logging.warning("Error querying %s: %s" % (url, e))
            else:
                # Soup-ify the html
                soup = BeautifulSoup(html)
                if '/careers/list' in url:
                    for datum in soup.find('div', class_='view-content').find_all('tr'):
                        title = datum.find('a').text
                        location = datum.find('div').text.strip().split(',')[0] 
                        link = datum.find('a')['href']
                        link = Link.canonicalize(link)
                        if self.conn_obj.count_urls(link) == 0:
                            logging.info('%s--New Job found: %s (%s), Link: %s' % (thread_name, title, location, link))
                            self.conn_obj.add_url(link) # add to database
                            tweet = 'New Job (%s): %s %s' % (location, title, link)
                            self.twitter.queue_new(tweet) # queue tweet
                        else:
                            logging.info('Existing Job found: %s, Link: %s' % (title, link))
                elif '/news' in url:
                    for datum in soup.find('div', class_='view-content').find_all('div', class_='views-row'):
                        title = datum.find('h2').text
                        link = datum.find('h2').find('a')['href']
                        link = Link.canonicalize(link)
                        if self.conn_obj.count_urls(link) == 0:
                            logging.info('%s--New Article found: %s, Link: %s' % (thread_name, title, link))
                            self.conn_obj.add_url(link) # add to database
                            tweet = 'New Article: %s %s' % (title, link)
                            self.twitter.queue_new(tweet) # queue tweet
                        else:
                            logging.info('Existing Article found: %s, Link: %s' % (title, link))
                elif '/media' in url:
                    for datum in soup.find('div', class_='group-right').find_all('div', class_='views-row'):
                        link = datum.find('a')['href']
                        link = Link.canonicalize(link)
                        if self.conn_obj.count_urls(link) == 0:
                            try:
                                media_html = u2.urlopen(link).read().decode('utf-8')
                            except Exception as e:
                                logging.info("Error querying for new media title at %s" % (link,))
                            else:
                                media_soup = BeautifulSoup(media_html)
                                title = media_soup.find('h1').text
                                media_type = media_soup.find('div', class_="breadcrumb").find('span', class_="last").text.split(' ')[0]
                                logging.info('%s--New %s found: %s, Link: %s' % (thread_name, media_type, title, link))
                                self.conn_obj.add_url(link) # add to database
                                tweet = 'New %s: %s %s' % (media_type, title, link)
                                self.twitter.queue_new(tweet) # queue tweet
                        else:
                            logging.info('Existing Media found: %s' % (link,))

            self.queue.task_done()

class Link():
    @staticmethod
    def canonicalize(url):
        if url[0] == '/': # relative url
            url = 'spacex.com' + url
        # remove mixed types and appended forwardslashes
        url = url.replace('http://','').replace('https://','').replace('www.','').strip('/') 
        return 'http://' + url

class Connection():

    def __init__(self, file='db.db'):
        self.db = file
        self.conn = sqlite3.connect(self.db)

    def create_table(self):
        self.conn.cursor().execute('''CREATE TABLE IF NOT EXISTS spacex (
                                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                                     link TEXT)''')
    
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

    def add_url(self, url):
        sql = 'INSERT INTO spacex (link) values (?)'
        cur = self.conn.cursor()
        cur.execute(sql, (url,))
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

    def queue_new(self, tweet):
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
    queue.put('http://www.spacex.com/careers/list')
    queue.put('http://www.spacex.com/news')
    queue.put('http://www.spacex.com/media')
    tweet_queue = SetQueue()
    num_workers = 5 
    for i in range(num_workers):
        t = Thready(queue, tweet_queue)
        t.setDaemon(True)
        t.start()
    queue.join() # wait for threads to finish
    logging.info('SpaceXNews.py is finished searching at %s' % datetime.datetime.now())
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
