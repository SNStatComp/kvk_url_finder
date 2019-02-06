import scrapy


class CompanySpider(scrapy.Spider):
    name = "company_spider"

    def __init__(self, *args, **kwargs):
        super(CompanySpider, self).__init__(*args, **kwargs)

        self.start_urls = kwargs.get('urls')

        postcodes = kwargs.get('postcodes')
        self.re_exp = "|".join(postcodes)

    def parse(self, response):
        zipcodes = response.xpath('string(//body)').re(self.re_exp)
