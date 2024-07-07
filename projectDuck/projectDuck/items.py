# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ProjectDuckItem(scrapy.Item):
    kingdom = scrapy.Field()
    phylum = scrapy.Field()
    classes = scrapy.Field()
    order = scrapy.Field()
    family = scrapy.Field()
    genus = scrapy.Field()
    specie = scrapy.Field()
    
    pass
