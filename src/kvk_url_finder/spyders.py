import scrapy


class CompanySpider(scrapy.Spider):
    name = "companyinfo"

    def start_resuest(self, urls):
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        self.log("PARSE")


