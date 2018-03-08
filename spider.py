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

    >>> isinstance(readCommand(["-u", "http://www.sina.com.cn"]), dict)
    True

    >>> readCommand(["-u", "http://www.sina.com.cn"])['url']
    'http://www.sina.com.cn'

    >>> readCommand(["-u", "www.sina.com.cn", "-d", "1"])['depth']
    1

    >>> readCommand(["-u", "www.sina.com.cn", "-f", "test.log"])['logfile']
    'test.log'

    >>> readCommand(["-u", "www.sina.com.cn", "-l", "1"])['loglevel']
    1
    
    >>> readCommand(["-u", "www.sina.com.cn", "--thread", "5"])['threadnumber']
    5

    >>> readCommand(["-u", "www.sina.com.cn", "--dbfile=test.db"])['dbfile']
    'test.db'

    >>> readCommand(["-u", "www.sina.com.cn", "--key=html"])['key']
    'html'
    """
    parser = OptionParser()
    parser.add_option("-u", dest="url", 
                        help="爬虫开始地址")
    parser.add_option("-d", type="int", dest="depth", default=2, 
                        help="爬虫深度，默认 2 层")
    parser.add_option("-f", dest="logfile", default="spider.log", 
                        help="日志文件名")
    parser.add_option("-l", type="int", dest="loglevel", default=5, 
                        help="日志文件记录详细程度(1-5)，数字越大越详细", metavar="1-5")
    parser.add_option("--testself", action="store_true", dest="testflag", default=False, 
                        help="启动自测")
    parser.add_option("--thread", type="int", dest="threadnumber", default=10, 
                       help="线程池大小，默认10个线程", metavar="THREAD_NUMBER")
    parser.add_option("--dbfile", dest="dbfile", default="spider.db", 
                        help="数据库文件名", metavar="DATABASE_FILE")
    parser.add_option("--key", dest="key", default=None, 
                       help="设定关键词，仅获取包含关键词的网页", metavar="KEYWORDS")
    (options, otherargs) = parser.parse_args(argv)

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
    """

    初始化实例

    """
    def __init__(self, **args):
        logging.debug('__Spider.__init____')
        self.index = 0
        self.process = 0
        self.init_url = args['url']
        self.depth = args['depth']
        self.key = args['key']
        self.dbfile = args['dbfile']
        self.threadnumber = args['threadnumber']
        self.q = Queue()
        self.used_set = set()
        logging.debug("init url: %s, depth: %s" % (self.init_url, self.depth))
        
    """

    处理初始界面

    包括将初始界面放入队列，创建线程，等待线程结束

    """
    def handleInitUrl(self):
        logging.debug('__Spider.handleInitUrl__ : ' + self.init_url)
        logging.debug('init Database')
        self.initDatabase(dbfile = self.dbfile)

        self.used_set.add(self.init_url)
        self.q.put((self.init_url, 1))
        
        if self.depth > 1:
            self.printProcess()
        
        threads = []
        for i in range(self.threadnumber):
            t = threading.Thread(target=self.run)
            t.start()
            threads.append(t)
        
        self.q.join()
        
        for i in range(self.threadnumber):
            self.q.put(None)
        for t in threads:
            t.join()
        
        # 所有线程结束后输出信息
        print("已爬取 %s 个urls, 当前线程池中尚有 0 个 urls 等待爬取" % self.index)
    
    """

    初始化数据库

    如果数据库已存在则 logging error

    """
    def initDatabase(self, dbfile):
        logging.debug('__Spider.initDatabase__')
        with sqlite3.connect(self.dbfile) as conn:
            logging.debug('successfully open database : ' + self.dbfile)
            c = conn.cursor()
            try:
                c.execute('''CREATE TABLE WEBSITE
                        (URL        TEXT    PRIMARY KEY     NOT NULL,
                        CONTENT     TEXT);''')
                logging.debug("Succeed creating table.")
            except Exception as e:
                logging.error("Fail to create table : " + str(e))
            conn.commit()      
    
    """

    将数据插入数据库，主关键字重复时更新数据库

    """

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
            except sqlite3.IntegrityError:
                c.execute("REPLACE INTO WEBSITE(URL, CONTENT) \
                VALUES (?, ?)", value)
                conn.commit()
                logging.info("Replace the duplicate key into table. ")
            except Exception as e:
                logging.error("Fail to insert table. " + str(e))
    """

    爬取 url 界面，返回界面信息

    """
    def webCrawler(self, url):
        logging.debug('__Spider.webCrawler__')
        logging.info('url : %s ' % url)
        try:
            response = request.urlopen(url, timeout=10)
            html_doc = response.read()
            logging.debug('successfully request this url.')
        except Exception as e:
            logging.error('fail to request this url, ' + str(e))
            return None
        
        return html_doc
    
    """

    线程开始运行，process 用于记录正在运行的线程数，结束时执行 task_done()
    
    """
    def run(self):
        while True:
            item = self.q.get()
            self.process = self.process + 1
            if item is None:
                break
            url, depth = item
            self.handling(url, depth)
            logging.debug("task done. url : %s", url)
            self.process = self.process - 1
            self.q.task_done() 
    
    """

    url 处理函数，调用 webCrawler 爬取 url，对内容进行解析，插入数据库
    并将内容中的超链接放入队列等待爬取

    """
    def handling(self, url, depth):
        logging.debug("request url, depth = " + str(depth))
        html_doc = self.webCrawler(url)
        
        if html_doc is None:
            return
        
        logging.debug('resolve the html.')
        soup = BeautifulSoup(html_doc.decode('utf-8', 'ignore'), "lxml")
        
        if ((self.key is None) or
            ((self.key is not None) and (soup.find_all(text=key)))):
            logging.debug("insert into database")
            self.insertDatabase(url=url, content=html_doc)
        
        self.index = self.index + 1
        
        if depth >= self.depth:
            return
        
        logging.debug('get urls with href attributes.')
        for link in soup.find_all(href=re.compile(r'^http')):
            if link.get('href') in self.used_set:
                logging.info('deplicate urls.')
                continue
            self.used_set.add(link.get('href').strip())
            self.q.put((link.get('href').strip(), depth+1))
    
    """

    printProcess() 用于再屏幕上输出爬取进度，
    printProcess() 1秒后显示，然后循环调用 printInfo() 每隔十秒显示一次

    """
    def printProcess(self):
        global t
        t = threading.Timer(1, self.printInfo)
        t.daemon = True
        t.start()
        
    def printInfo(self):
        print("已爬取 %s 个urls, 当前线程池中尚有 %s 个 urls 等待爬取" % (self.index, self.process)) 

        global t
        t = threading.Timer(10, self.printInfo)
        t.start()

"""

主函数

"""
if __name__ == "__main__":
    """
    从命令行调用 spider.py

    > python spider.py

    添加 -h 或 --help 参数查看更多帮助

    > python spider.py --help
    """
    args = readCommand( sys.argv[1:] )
    
    if args['testflag']:
        import doctest
        doctest.testmod(verbose=True)
        exit()

    # change 1,2,3,4,5 to 50,40,30,20,10
    loglevel = -10 * args["loglevel"] + 60
    logging.basicConfig(filename=args["logfile"], filemode = 'w', level=loglevel)
    logging.debug('log file is: %s, log level is: %d' %(args["logfile"], args["loglevel"]))
    
    if args["url"]:
        if not args["url"].startswith("http"):
            args["url"] = "http://" + args["url"]
    else:
        raise ValueError("Please input the url.")

    spider = Spider(**args)
    spider.handleInitUrl()