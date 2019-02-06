import scrapy
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from twisted.internet import reactor

from kvk_url_finder.electronics import ElectronicsSpider

configure_logging({"LOG_FORMAT": '%(levelname)s: %(message)'})
runner = CrawlerRunner()

d = runner.crawl(ElectronicsSpider)
d.addBoth(lambda _: reactor.stop())
reactor.run()
