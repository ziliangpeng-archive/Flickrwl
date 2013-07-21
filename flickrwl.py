#!/usr/bin/env python

'''
Created on Sep 25, 2010

@author: ziliangdotme
'''
import re, urllib2, os, thread, sys, threading
from ThreadPool import ThreadPool
from util import syncPrint, create_path

# (TODO) further tunes regex patterns
# (TODO) fix variable names and code style
# (TODO) add support to read operations from stdin


# common utilities
class BasePhotoCrawler(threading.Thread):
    
    def __init__(self, num_threads=42):
        super(BasePhotoCrawler, self).__init__()
        self.finished = threading.Event()
        self.crawled = set()
        self.pool = ThreadPool(num_threads, BasePhotoCrawler.download_and_save_image_file)
        self.start()
        

    @staticmethod
    def fetch(url, maximum_try=5):
        for _ in range(maximum_try):
          try:
              html = urllib2.urlopen(url).read()
              return html
          except BaseException, e:
              syncPrint('Error: ' + str(e) + '\n' + 'on url: ' + url)
              # (TODO) INFO level log
        # (TODO) ERROR level log
        

    @staticmethod
    def getPhotoId(url):
        '''get photo id from a url in the format of /photos/{username}/{id}/'''
        pattern = r'/photos/[^/]+/(\d+)/'
        # (TODO) Log if fail
        return re.search(pattern, url).group(1)
            

    def downloadImage(self, url, path):
        if url in self.crawled:
            return
        self.crawled.add(url)
        self.pool.add_data(url, path)


    @staticmethod
    def download_and_save_image_file(url, path):
        '''download image from a image page(not necessarily a .jpg)'''
        create_path(path)
        if os.path.isfile(path):
            syncPrint('%-64s has been downloaded to %s, skip.' % (url, path))
            return
                
        photoID = BasePhotoCrawler.getPhotoId(url)
        doc = BasePhotoCrawler.fetch(url + 'sizes/')
        pattern = r'<img[^>]+src=\"(http://\w+\.staticflickr\.com/\w+/{id}[^>]+\.(jpg|png|gif))[^>]*>'.format(id=photoID)
        try:
          m = re.search(pattern, doc).group(1)
          syncPrint('downloading   %-64s  to   %s' % (url, path))
          img = BasePhotoCrawler.fetch(m)
          open(path, "w+").write(img)
        except BaseException, e:
          syncPrint('Error: ' + str(e) + '\n' + 'on url: ' + url + '\n' + 'pattern: ' + pattern + '\n' + doc)
          # (TODO) break down exception handling
          print 'Error: no regex match in %s' % url


    @staticmethod
    def getPhotoLinksFromPage(doc):
        '''get all links to photos within a HTML document'''
        pattern = r'/photos/[^/]+/\d+/'
        return set('http://www.flickr.com' + photo_url for photo_url in re.findall(pattern, doc))
    

    @staticmethod
    def hasNextPage(pageNumber, doc):
        '''Algorithm: check whether there's a <a> tag with src contains 'page' + str(pageNumber + 1) in it'''
        pattern = r'<a[^>]+href=\"[^\"]*page=?{nextPage}[^>]*>'.format(nextPage=pageNumber+1)
        return bool(re.search(pattern, doc))
        

    def crawlSinglePage(self, url, path):
        doc = self.fetch(url)
        for url in self.getPhotoLinksFromPage(doc):
            self.downloadImage(url, path.format(id=self.getPhotoId(url)))
        return doc


    def crawlPages(self, url, path, end=1024, begin=1):
        for pageNumber in xrange(begin, end):
            if self.finished.is_set():
                break
            doc = self.crawlSinglePage(url.format(pageNumber=pageNumber), path)
            if not BasePhotoCrawler.hasNextPage(pageNumber, doc):
                break


    def run(self):
        self.crawl()


    def shutdown(self):
        self.finished.set()


    def finish(self):
        self.shutdown()
        self.pool.shutdown()


class UserCrawler(BasePhotoCrawler):

    def __init__(self, username):
        self.username = username
        super(UserCrawler, self).__init__()
    
    def getAllSets(self, doc=None):
        '''retrieve all sets in sets page, there might be more than one page, this method did not handle this case.'''
        if doc is None:
            url = r'http://www.flickr.com/photos/{username}/sets/'.format(username=self.username)
            doc = self.fetch(url)
        pattern = r'<a[^>]+href=\"/photos/{username}/sets/(\d+)/[^>]+title=\"(.*?)\"[^>]*>'.format(username=self.username)
        map = {} 
        for match in re.finditer(pattern, doc):
            setId = match.group(1)
            setName = match.group(2)
            map[setId] = setName
            syncPrint('Found set id={setId} name={setName}'.format(setId=setId, setName=setName))
        return map
    
    def crawl(self):
        # crawl all sets
        map = self.getAllSets()
        for setId, setName in map.iteritems():
            url = r'http://www.flickr.com/photos/{username}/sets/{setId}/page{pageNumber}/'.format(username=self.username, setId=setId, pageNumber='{pageNumber}')
            path = r'Flickr/User/{username}/{setName}/{id}.jpg'.format(username=self.username.replace('/', '-'), setName=setName.replace('/', '-'), id='{id}')
            self.crawlPages(url, path)
                
        # crawl all photos
        url = r'http://www.flickr.com/photos/{username}/page{pageNumber}'.format(username=self.username, pageNumber='{pageNumber}')
        path = r'Flickr/User/{username}/{id}.jpg'.format(username=self.username, id='{id}')
        self.crawlPages(url, path)
        

class TagCrawler(BasePhotoCrawler):
    
    def __init__(self, tag):
        self.tag = str(tag).lower()
        super(TagCrawler, self).__init__()
            
    def crawl(self):
        url = r'http://www.flickr.com/photos/tags/{tag}/?page={pageNumber}'.format(tag=self.tag, pageNumber='{pageNumber}')
        path = r'Flickr/Tag/{tag}/{id}.jpg'.format(tag=self.tag, id='{id}')
        self.crawlPages(url, path)
            

class SearchResultCrawler(BasePhotoCrawler):
    
    def __init__(self, keys):
        if type(keys).__name__ == 'str': key = str(keys).lower()
        elif type(keys).__name__ == 'list': key = '+'.join(keys).lower()
        else: syncPrint(type(keys).__name__)
        self.key = key
        super(SearchResultCrawler, self).__init__()
        
    def crawl(self):
        url = r'http://www.flickr.com/search/?q={key}&page={pageNumber}'.format(key=self.key, pageNumber='{pageNumber}')
        path = r'Flickr/Search/{key}/{id}.jpg'.format(key=self.key.replace('+', '_'), id='{id}')
        self.crawlPages(url, path)


# main
if __name__ == '__main__':
    ready = False

    if len(sys.argv) >= 2:
        op = sys.argv[1]
    else:
        op = ''

    if str(op).lower() == 'user': # user
        if len(sys.argv) == 3:
            username = sys.argv[2]
            ready = True
            crawler = UserCrawler(username)
    elif str(op).lower() == 'tag': # tag
        if len(sys.argv) == 3:
            tag = sys.argv[2]
            ready = True
            crawler = TagCrawler(tag)
    elif str(op).lower() == 'search': # search
        if len(sys.argv) >= 3:
            keywords = sys.argv[2:]
            ready = True
            crawler = SearchResultCrawler(keywords)

    if not ready:
        syncPrint('invalid usage, read README.')
        sys.exit(1)


    # Let the crawler finish after an input.
    # (TODO) need to find a better way to interact with user.
    # Currently the program expect an input to exit, even when crawling finish for all
    # photos(eg. user crawling).
    raw_input()
    syncPrint('Exiting...')
    crawler.finish()



