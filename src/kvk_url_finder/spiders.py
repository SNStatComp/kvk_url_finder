import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule
import re
import datetime

import re
from six.moves.urllib.parse import urlparse

import scrapy
from scrapy.http import Request, HtmlResponse
from scrapy.linkextractors import LinkExtractor


class Page(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    size = scrapy.Field()
    referer = scrapy.Field()
    newcookies = scrapy.Field()
    body = scrapy.Field()
    postcodes = scrapy.Field()
    kvknummers = scrapy.Field()
    datetime = scrapy.Field()
    matches = scrapy.Field()


class CompanySpider(scrapy.Spider):
    name = "company_spider"
    rules = (
        Rule(LinkExtractor(allow=(),
                           restrict_css=('.pageNextPrev')),
             callback="parse",
             follow=True),
    )

    custom_settings = {
        "DEPTH_LIMIT": 10
    }

    def __init__(self, **kw):

        super(CompanySpider, self).__init__(**kw)

        self.urls = kw.get('urls')
        self.regexp = kw.get('regexp', [])

        if not isinstance(self.urls, list):
            self.urls = [self.urls]
        if not isinstance(self.regexp, list):
            self.regexp = [self.regexp]

        self.allowed_domains = None
        for cnt, url in enumerate(self.urls):
            if not url.startswith('http://') and not url.startswith('https://'):
                url = 'http://%s/' % url
                self.urls[cnt] = url
            if self.allowed_domains is None:
                self.allowed_domains = [re.sub(r'^www\.', '', urlparse(url).hostname)]
        self.link_extractor = LinkExtractor()
        self.cookies_seen = set()

    def start_requests(self):
        for url in self.urls:
            yield scrapy.Request(url=url, callback=self.parse, dont_filter=True)

    def parse(self, response):
        """Parse a PageItem and all requests to follow

        @url http://www.scrapinghub.com/
        @returns items 1 1
        @returns requests 1
        @scrapes url title foo
        """
        page = self._get_item(response)
        r = [page]
        r.extend(self._extract_requests(response))
        return r

    def _get_item(self, response):
        item = Page(
            url=response.url,
            size=str(len(response.body)),
            referer=response.request.headers.get('Referer'),
            datetime=datetime.datetime.now()
        )
        self._set_title(item, response)
        self._set_new_cookies(item, response)
        self._set_company_info(item, response)
        return item

    def _extract_requests(self, response):
        r = []
        if isinstance(response, HtmlResponse):
            links = self.link_extractor.extract_links(response)
            r.extend(Request(x.url, callback=self.parse) for x in links)
        return r

    def _set_title(self, page, response):
        if isinstance(response, HtmlResponse):
            title = response.xpath("//title/text()").extract()
            if title:
                page['title'] = title[0]

    def _set_new_cookies(self, page, response):
        cookies = []
        for cookie in [x.split(b';', 1)[0] for x in
                       response.headers.getlist('Set-Cookie')]:
            if cookie not in self.cookies_seen:
                self.cookies_seen.add(cookie)
                cookies.append(cookie)
        if cookies:
            page['newcookies'] = cookies

    def _set_company_info(self, page, response):
        body_text = response.xpath('//body//text()')
        zipcodes = body_text.re(r"\b\d{4}\s{0,1}[a-zA-Z]{2}\b")
        zipcodes = [re.sub("\s", "", x).upper() for x in zipcodes]
        kvk_numbers = body_text.re(r"\b\d{7,8}\b")
        matches = []
        for regexp in self.regexp:
            try:
                matches.extend(body_text.re(regexp))
            except KeyError:
                matches = body_text.re(regexp)
        try:
            page["postcodes"].extend(zipcodes)
        except KeyError:
            page["postcodes"] = zipcodes
        try:
            page["kvknummers"].extend(kvk_numbers)
        except KeyError:
            page["kvknummers"] = kvk_numbers
        try:
            page["matches"].extend(matches)
        except KeyError:
            page["matches"] = matches
