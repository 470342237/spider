#!/usr/bin/env python2
# -*- coding:utf-8 -*-
import os
import sys
import time
import urllib2
import re
import logging
import sqlite3
from sets import Set
from optparse import OptionParser
from bs4 import BeautifulSoup

def readCommand( argv ):
    """
    解析命令行参数
    """
    parser = OptionParser()
    parser.add_option("-u", dest="url", 
                        help=u"爬虫开始地址")
    parser.add_option("-d", type="int", dest="deep", 
                        help=u"爬虫深度，默认 1 层")
    parser.add_option("-f", dest="logfile", default="spider.log", 
                        help=u"log 文件名")
    parser.add_option("-l", dest="loglevel", default=3, 
                        help=u"日志文件记录详细程度(1-5)，数字越大越详细", metavar="1-5")
    parser.add_option("--testself", action="store_true", dest="testflag", default=False, 
                        help=u"启动自测")
    #parser.add_option("--thread", type="int", dest="threadnumber", default=10, 
    #                   help=u"线程池大小，默认10个线程", metavar="THREAD_NUMBER")
    parser.add_option("--dbfile", dest="dbfile", default="spider.db", 
                        help=u"数据库文件名", metavar="DATABASE_FILE")
    #parser.add_option("--key", dest="key",
    #                   help=u"设定关键词，仅获取包含关键词的网页", metavar="KEYWORDS")
    (options, otherargs) = parser.parse_args()
    
    #
    args = {}
    args['url'] = options.url
    args['deep'] = options.deep
    args['logfile'] = options.logfile
    args['loglevel'] = options.loglevel
    args['testflag'] = options.testflag
    args['dbfile'] = options.dbfile
    
    return args

def webCrawler(url):
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}  
    try:
        request = urllib2.Request(url, headers = headers)
        response = urllib2.urlopen(request)
        html_doc = response.read()
        logging.debug('successfully request this url.')
    except UnicodeEncodeError, e:
        logging.error('UnicodeEncodeError in : ' + url)
    except urllib2.HTTPError, e:
        logging.error('HTTPError %s in : %s' % (e.code, url))
        return None
    except urllib2.URLError, e:
        logging.error('URLError %s in : %s' % (e.reason, url))
        return None
    return html_doc

def initDatabase(dbfile):
    with sqlite3.connect(dbfile) as conn:
        logging.debug('successfully open database : ' + dbfile)
        c = conn.cursor()
        try:
            c.execute('''CREATE TABLE WEBSITE
                    (ID     INT     PRIMARY KEY     NOT NULL,
                    URL     TEXT                    NOT NULL,
                    CONTENT TEXT);''')
            logging.info("Succeed creating table.")
        except:
            logging.info("Fail to create table.")
        conn.commit()
    global index
    index = 0
    
def insertDatabase(id, url, html_doc, dbfile):
    global index
    index = index + 1
    logging.debug("Begin to insert Database")
    with sqlite3.connect(dbfile) as conn:
        c = conn.cursor()
        #try:
        value = (index, url, html_doc)
        c.execute("INSERT OR IGNORE INTO WEBSITE(ID, URL, CONTENT) \
        VALUES (?, ?, ?)" % value)
        conn.commit()
        logging.info("Succeed inserting table.")
        #except:
        #    logging.error("Fail to insert table, ")
    
def search(url, deep, used_set, dbfile):
    logging.debug("url : " + url)
    logging.info('request this url.')   
    html_doc = webCrawler(url)    
    if html_doc is None:
        return 
    logging.info('insert into database.')
    insertDatabase(len(used_set), url, html_doc, dbfile)
    
    logging.info('resolve the html.')
    soup = BeautifulSoup(html_doc, "lxml")
    
    if deep==1:
        return
    current_set = Set()
    for link in soup.find_all(attrs={"href":re.compile(r'^http')}):
        if link.get('href') in used_set:
            continue
        # logging.debug('%d : %s' %(len(used_set), link.get('href')))
        used_set.add(link.get('href'))
        current_set.add(link.get('href'))
    logging.info("There're %d hrefs in current url" % len(current_set))
    for link in current_set:
        if link is None:
            continue
        time.sleep(0.2)
        search(link, deep-1, used_set, dbfile)
    
def spider(url, deep, logfile, loglevel, testflag, dbfile):
    if testflag:
        import doctest
        doctest.testmod()
    FORMAT = "%(message)s"
    logging.basicConfig(filename=logfile, filemode = 'w', level=loglevel, format=FORMAT)
    logging.info('log file is: %s, log level is: %d' %(logfile, loglevel))
    logging.getLogger("chardet").setLevel(logging.WARNING)
    
    logging.info('the initial url is : ' + url)
    
    # 记录已经访问的 url
    logging.info('create used_set')
    used_set = Set()
    used_set.add(url)
    
    logging.info('init Database')
    conn = initDatabase(dbfile)
    
    logging.info('begin search of deep: %d' % deep)
    search(url, deep, used_set, dbfile)
    logging.info("search ended. There're %d hrefs in total" % len(used_set))
    
if __name__ == "__main__":
    """
    从命令行调用 spider.py

    > python spider.py

    添加 -h 或 --help 参数查看更多帮助

    > python spider.py --help
    """
    args = readCommand( sys.argv[1:] )
    spider(**args)