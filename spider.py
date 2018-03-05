#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import os
import sys
import time
from urllib import request
import re
import logging
import sqlite3
import threading
from optparse import OptionParser
from bs4 import BeautifulSoup
from queue import Queue

def readCommand( argv ):
    """
    解析命令行参数
    """
    parser = OptionParser()
    parser.add_option("-u", dest="url", 
                        help=u"爬虫开始地址")
    parser.add_option("-d", type="int", dest="depth", default=2, 
                        help=u"爬虫深度，默认 2 层")
    parser.add_option("-f", dest="logfile", default="spider.log", 
                        help=u"日志文件名")
    parser.add_option("-l", dest="loglevel", default=4, 
                        help=u"日志文件记录详细程度(1-5)，数字越大越详细", metavar="1-5")
    parser.add_option("--testself", action="store_true", dest="testflag", default=False, 
                        help=u"启动自测")
    parser.add_option("--thread", type="int", dest="threadnumber", default=10, 
                       help=u"线程池大小，默认10个线程", metavar="THREAD_NUMBER")
    parser.add_option("--dbfile", dest="dbfile", default="spider.db", 
                        help=u"数据库文件名", metavar="DATABASE_FILE")
    parser.add_option("--key", dest="key", default=None, 
                       help=u"设定关键词，仅获取包含关键词的网页", metavar="KEYWORDS")
    (options, otherargs) = parser.parse_args()
    
    args = dict()
    args['url'] = options.url
    args['depth'] = options.depth
    args['logfile'] = options.logfile
    args['loglevel'] = options.loglevel
    args['testflag'] = options.testflag
    args['threadnumber'] = options.threadnumber
    args['dbfile'] = options.dbfile
    args['key'] = options.key
    
    return args

class Spider(object):
    def __init__(self, **args):
        logging.debug('__Spider.__init____')
        self.init_url = args['url']
        self.depth = args['depth']
        self.key = args['key']
        self.dbfile = args['dbfile']
        self.threadnumber = args['threadnumber']
        self.key = args['key']
        self.q = Queue()
        self.used_set = set()
          
    def handleInitUrl(self):
        logging.debug('__Spider.handleInitUrl__ : '+self.init_url)
        logging.info('init Database')
        self.initDatabase(dbfile = self.dbfile)

        self.used_set.add(self.init_url)
        self.q.put((self.init_url, 0))
        
        self.handling(url=self.init_url, depth=0)
        
        for i in range(self.threadnumber):
            t = threading.Thread(target=self.run)
            t.start()
        
        self.q.join()
        
    def initDatabase(self, dbfile):
        logging.debug('__Spider.initDatabase__')
        with sqlite3.connect(self.dbfile) as conn:
            logging.debug('successfully open database : ' + self.dbfile)
            c = conn.cursor()
            try:
                c.execute('''CREATE TABLE WEBSITE
                        (URL     TEXT    PRIMARY KEY     NOT NULL,
                        CONTENT TEXT);''')
                logging.info("Succeed creating table.")
            except Exception as e:
                logging.error("Fail to create table : " + str(e))
            conn.commit()      
        
    def insertDatabase(self, url, content):
        logging.debug('__Spider.insertDatabase__')
        with sqlite3.connect(self.dbfile) as conn:
            logging.debug('successfully open database : ' + self.dbfile)
            c = conn.cursor()
            try:
                value = (url, content)
                c.execute("INSERT INTO WEBSITE(URL, CONTENT) \
                VALUES (?, ?)", value)
                conn.commit()
                logging.debug("Succeed inserting table. ")
            except Exception as e:
                logging.warning("Fail to insert table. " + str(e))
        
    def webCrawler(self, url):
        logging.debug('__Spider.webCrawler__')
        logging.info('url : ' + url)
        try:
            response = request.urlopen(url)
            html_doc = response.read()
            logging.debug('successfully request this url.')
        except Exception as e:
            logging.error('fail to request this url.' + str(e))
            return None
        
        return html_doc
        
    def run(self):
        while True:
            url, depth = self.q.get()
            self.handling(url, depth)
            self.q.task_done()
                
    def handling(self, url, depth):
        logging.info("request url, depth = " + str(depth))
        html_doc = self.webCrawler(url)
        
        if html_doc is None:
            return
        
        logging.info('resolve the html.')
        soup = BeautifulSoup(html_doc.decode('utf-8', 'ignore'), "lxml")
        
        if ((self.key is None) or
            ((self.key is not None) and (soup.find_all(text=key)))):
            logging.info("insert into database")
            self.insertDatabase(url=url, content=html_doc)
        
        if depth == self.depth:
            return
        
        logging.debug('get the urls from href attributes.')
        for link in soup.find_all(href=re.compile(r'^http')):
            if link.get('href') in self.used_set:
                continue
            self.used_set.add(link.get('href'))
            self.q.put((link.get('href'), depth+1))
        

if __name__ == "__main__":
    """
    从命令行调用 spider.py

    > python spider.py

    添加 -h 或 --help 参数查看更多帮助

    > python spider.py --help
    """
    args = readCommand( sys.argv[1:] )
    
    loglevel = -10*args["loglevel"] + 60
    logging.basicConfig(filename=args["logfile"], filemode = 'w', level=args["loglevel"])
    logging.debug('log file is: %s, log level is: %d' %(args["logfile"], args["loglevel"]))
    
    if args["testflag"]:
        import doctest
        doctest.testmod()
    
    spider = Spider(**args)
    spider.handleInitUrl()