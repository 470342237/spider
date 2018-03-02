#!/usr/bin/env python2
# -*- coding:utf-8 -*-

import sys
import time
import urllib2
import re
from sets import Set
from bs4 import BeautifulSoup

def readCommand( argv ):
    """
    解析命令行参数
    """
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-u", dest="url",
						help="爬虫开始地址")
    parser.add_option("-d", type="int", dest="deep",
						help="爬虫深度，默认 1 层")
    #parser.add_option("-f", dest="logfile",
	#					help="log 文件名")
	#parser.add_option("-l", dest="loglevel",
    #                   help="日志文件记录详细程度(1-5)，数字越高越详细", metavar="")
	#parser.add_option("--testself", action="store_true", dest="testflag", default=False, 
    #                   help="启动自测")
	#parser.add_option("--thread", type="int", dest="threadnumber", default=10, 
	#					help="线程池大小，默认10个线程", metavar="THREAD_NUMBER")
	#parser.add_option("--dbfile", dest="dbfile",
	#					help="数据库文件名", metavar="DATABASE_FILE")
	#parser.add_option("--key", dest="key",
	#					help="设定关键词，仅获取包含关键词的网页", metavar="KEYWORDS")
    (options, otherargs) = parser.parse_args()
    
    #
    args = {}
    args['url'] = options.url
    args['deep'] = options.deep
    
    return args

def webCrawler(url):
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}  
    try:
        response = urllib2.urlopen(url, timeout=20)
        if response.info().get('Content-Encoding') == 'gzip':
            buf = StringIO(response.read())
            f = gzip.GzipFile(fileobj=buf)
            html_doc = f.read()
        else:
            html_doc = response.read()
        
        #request = urllib2.Request(url, headers = headers)
        #response = urllib2.urlopen(request)
        #html_doc = response.read()
    except UnicodeEncodeError, e:
        print url
    except urllib2.HTTPError, e:
        print url, e.code
        return None
    except urllib2.URLError, e:
        print e.reason
        return None
    return html_doc
    
def search(url, deep, used_set):
    html_doc = webCrawler(url)
    if html_doc is None:
        return 
    soup = BeautifulSoup(html_doc, "lxml")
    if deep==1:
        return
    current_set = Set()
    for link in soup.find_all(attrs={"href":re.compile(r'^http:')}):
        if link.get('href') in used_set:
            continue
        # print len(used_set) # link.get('href')
        used_set.add(link.get('href'))
        current_set.add(link.get('href'))
    print len(current_set), len(used_set)
    for link in current_set:
        if link is None:
            continue
        time.sleep(0.2)
        search(link, deep-1, used_set)
    
def spider(url, deep):
    used_set = Set()
    used_set.add(url)
    
    print url
    print len(used_set)
    
    search(url, deep, used_set)
    print len(used_set)
    
if __name__ == "__main__":
    """
    从命令行调用 spider.py

    > python spider.py

    添加 -h 或 --help 参数查看更多帮助

    > python spider.py --help
    """
    args = readCommand( sys.argv[1:] )
    spider(**args)