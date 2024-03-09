# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ProjectDuckItem(scrapy.Item):
    kingdom = scrapy.Field()
    phylum = scrapy.Field()
    classes = scrapy.Field()
    url = scrapy.Field()
    data = scrapy.Field()
    data_url = scrapy.Field()
    pass
