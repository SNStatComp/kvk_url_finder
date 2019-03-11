from scrapy.linkextractor import LinkExtractor
from scrapy.spider import Rule, Spider


class CompanySpider(Spider):
    name = "company_spider"
    rules = (
        Rule(LinkExtractor(allow=(),
                           restrict_css=('.pageNextPrev')),
             callback="parse_item",
             follow=True),
    )

    def __init__(self, *args, **kwargs):
        super(CompanySpider, self).__init__(*args, **kwargs)

        self.start_urls = kwargs.get('urls', [])
        if isinstance(self.start_urls, str):
            self.start_urls = [self.start_urls]

        postcodes = kwargs.get('postcodes')
        self.re_exp = "|".join(postcodes)

    def parse(self, response):
        self.log("Scraping postcodes")
        zipcodes = response.xpath('string(//body)').re(self.re_exp)
        yield zipcodes

