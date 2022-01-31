import scrapy


class WhiskySpider(scrapy.Spider):
    name = 'whisky_auctioneer'

    start_urls = ['https://whiskyauctioneer.com/december-2021-auction?f%5B0%5D=distilleries%3A3']

    def parse(self, response, **kwargs):
        # bottle = response.xpath("//span[@class='protitle']/text()").extract()
        # price = response.xpath("//span[@class='WinningBid']/span[@class='uc-price']/text()").extract()
        # end_date = response.xpath("//div[@class='enddatein']/span[@class='uc-price']/text()").extract()
        #
        items = response.xpath("//div[contains(@class,'producthomepage')]")

        for item in items:
            bottle = item.xpath(".//span[@class='protitle']/text()").extract_first()
            price = item.xpath(".//span[@class='WinningBid']/span[@class='uc-price']/text()").extract_first()
            end_date = item.xpath(".//div[@class='enddatein']/span[@class='uc-price']/text()").extract_first()

            yield {
                'bottle': bottle,
                'price': price,
                'end_date': end_date,
            }

        next_page = response.xpath("//li[@class='pager-next']/a/@href").extract_first()
        next_page = next_page.replace(next_page[-1:], str(int(next_page[-1:]) + 1))
        print(f"Next: {next_page}")

        if next_page and next_page[-1:] != '3':
            next_page = response.urljoin(next_page)
            print(f"Next after join: {next_page}")
            yield scrapy.Request(next_page, callback=self.parse)
