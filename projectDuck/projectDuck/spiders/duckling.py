import pandas
import scrapy
import json
import logging
import random
import socks
import socket
# from goose3 import Goose
from scrapy.spiders import CrawlSpider, Rule
from scrapy.crawler import CrawlerRunner
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
# from stem import Signal
# from stem.control import Controller
from collections import OrderedDict
from projectDuck.items import ProjectDuckItem
from twisted.internet import reactor, defer

# socks.set_default_proxy(socks.PROXY_TYPE_SOCKS5, addr='127.0.0.1', port=9050)
# socket.socket = socks.socksocket

class Duckling(CrawlSpider):
    name = "duckling"
    
    logging.basicConfig(
        filename='log.txt',
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.NOTSET
    )
    
    custom_settings = {"FEEDS": {"duckling.json": {"format": "json"}}}
    
    # life = [
    #     "http://158.69.212.254:80",
    #     "http://103.149.130.38:80",
    #     "http://180.235.65.13:80",
    #     "http://103.111.118.68:1080",
    #     "http://4.16.68.158:443"
    #     ]
    
    with open(r"C:\Users\APPKVc1000\projectDuck\duck.json"):
        domains = json.load(open(r"C:\Users\APPKVc1000\projectDuck\duck.json", encoding='utf-8'))
    
    kingdoms = dict()
    phyla = dict()
    phylum = dict()
    family = dict()
    genus = dict()
    
    for domain in domains:
        kingdoms.update({webpages : url for webpages, url in domain.items()
               if isinstance(url, str)})
        
    for domain in domains:
        phyla.update({webpages : data for webpages, data in domain.items()
               if isinstance(data, dict)}) 

    for webpage, url in phyla.items():
        for classes, order in url.items():
            phylum[(webpage, classes)] = order
        
    for classes, order in pandas.DataFrame.from_dict(phylum, orient='index').items():
        family.update({classes: list(set([
            *[specie for data in order.dropna().drop_duplicates().tolist() for specie in data if isinstance(specie, str)],
            *[webpage for data in [
                list(webpage.keys()) for data in order.dropna().drop_duplicates().tolist() for webpage in data if isinstance(webpage, dict)
            ] for webpage in data]
        ]))})
        genus.update({classes: list(set([
            *[url for data in [
                list(url.values()) for data in order.dropna().drop_duplicates().tolist() for url in data if isinstance(url, dict)
            ] for url in data]
        ]))})
    
    family = dict(OrderedDict(sorted(family.items(), key = lambda order : len(order[1]), reverse=True)))
    genus = dict(OrderedDict(sorted(genus.items(), key = lambda order : len(order[1]), reverse=True))) 

    def start_requests(self):
        for classes, order in self.genus.items():
            for url in order:
                # Controller.from_port(address='127.0.0.1', port=9051).authenticate(password="1AD0PK0P0c")
                # Controller.from_port(address='127.0.0.1', port=9051).signal(Signal.NEWNYM)
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    cb_kwargs=dict(data=classes),
                    # meta={'proxy': random.choice(life)}
                    )
    
    def parse(self, response, data):
        duckling = ProjectDuckItem()
        
        duck = dict()
        
        duckling['url'] = response.url
                
        duckling['classes'] = [pandas.Series(data.xpath(
            ".//descendant-or-self::*//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class=\"reference\"])]"
            ).extract()).str.cat().strip() for data in response.xpath(
                "//*[@class[contains(.,\"infobox-label\")]]"
                )]
     
        duckling['data'] = list()
        duckling['data_url'] = list()
        
        order = dict()
                
        for classes in duckling['classes']:
            
            class_nest = f"//*[@class[contains(.,\"infobox-label\")]]//descendant-or-self::*[contains(.,\"{classes}\")]"
            order_nest = "//ancestor-or-self::*[@class[contains(.,\"infobox-label\")]]//parent::*//*[@class[contains(.,\"infobox-data\")]]"
            
            duckling['data_url'] = list(
                filter(None, [
                    *[data for data in [dict(zip(
                        [family.strip() for family in response.xpath(
                            f"{class_nest}{order_nest}//descendant-or-self::*[@href and @title]//@title"
                            ).extract()],
                        [response.urljoin(genus) for genus in response.xpath(
                            f"{class_nest}{order_nest}//descendant-or-self::*[@href and @title]//@href"
                            ).extract()]
                        ))]],
                    *[data for data in [dict(zip(
                        [family.strip() for family in response.xpath(
                            f"{class_nest}{order_nest}//descendant-or-self::*[@href and not(@title)]//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class=\"reference\"])]"
                            ).extract()],
                        [family.strip() for family in response.xpath(
                            f"{class_nest}{order_nest}//descendant-or-self::*[@href and not(@title)]//@href[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class=\"reference\"])]"
                            ).extract()]
                        ))]]
                    ]))
   
            duckling['data'] = list(filter(None, [specie.strip() for specie in response.xpath(
                f"{class_nest}{order_nest}//descendant-or-self::*//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class=\"reference\"]) and not(ancestor-or-self::*[@href])]"
                ).extract()
                ]))
                        
            order.update({classes: list(filter(None, [
                *duckling['data_url'], *duckling['data']
                ]))})
        
        # if order:
        #     duck.update(data: {duckling['url']: [order, Goose({'keep_footnotes': False}).extract(raw_html=response.body).cleaned_text]}})
            
        # else:
        #     duck.update(data: {duckling['url']: Goose({'keep_footnotes': False}).extract(raw_html=response.body).cleaned_text}})
        
        if order:
            duck.update({data: {duckling['url']: order}})
        else:
            duck.update({data: duckling['url']})
            
        yield duck

configure_logging()
runner = CrawlerRunner(get_project_settings())

@defer.inlineCallbacks
def crawler():
    yield runner.crawl(Duckling)
    reactor.stop()

crawler()
reactor.run()
