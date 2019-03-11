import logging
import multiprocessing as mp

from scrapy.crawler import CrawlerProcess
from scrapy.signals import item_passed
from scrapy.utils.project import get_project_settings
from scrapy.xlib.pydispatch import dispatcher


class CrawlerWorker(mp.Process):
    """
    Scrapy CrawlerWorker is used to launch scrapy per url from a script.
    """
    name = "crawlerworker"

    def __init__(self, spider, result_queue):
        mp.Process.__init__(self)
        self.result_queue = result_queue
        self.items = list()
        self.spider = spider
        self.logger = logging.getLogger(self.name)

        settings = get_project_settings()
        self.logger.setLevel(logging.WARNING)
        self.logger.debug("Create CrawlerProcess with settings {}".format(settings))
        self.crawler = CrawlerProcess(settings)

        dispatcher.connect(self._item_passed, item_passed)

    def _item_passed(self, item):
        self.items.append(item)

    def run(self):
        self.logger.info("Start here with {}".format(self.spider.urls))
        self.crawler.crawl(self.spider, urls=self.spider.urls)
        self.crawler.start()
        self.crawler.stop()
        self.result_queue.put(self.items)
