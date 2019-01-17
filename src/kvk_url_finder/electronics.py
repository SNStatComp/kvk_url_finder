# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import (CrawlSpider, Rule)


class ElectronicsSpider(CrawlSpider):
    name = 'electronics'
    allowed_domains = ['www.olx.com.pk']
    start_urls = [
        'http://www.olx.com.pk/computer-accessories',
        'http://www.olx.com.pk/tv-video-audio',
        'http://www.olx.com.pk/games-entertainment'
    ]
    rules = (
        Rule(LinkExtractor(allow=(),
                           restrict_css=('.pageNextPrev')),
             callback="parse_item",
             follow=True),
    )

    def parse_item(self, response):
        print("Processing .. {}".format(response.url))
        item_links = response.css(".large.detailsLink::attr(href)").extract()
        for a in item_links:
            yield scrapy.Request(a, callback=self.parse_detail_page)
