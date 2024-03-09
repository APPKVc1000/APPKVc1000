# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from collections import defaultdict


class ProjectduckPipeline:
    def process_item(self, item, spider):
        
        if any(isinstance(data, dict) for data in item.values()):
        
            phyla = defaultdict(list)            
            for webpage, data in item.items():
                    for webpages, url in item.items():
                        phyla[webpages].append(url)
                
            phylum = dict()
            for website, data in phyla.items():
                for webpage in data:
                    for url, classes in webpage.items():
                        phylum[str((website, url))] = classes
            item = phylum
        
        return item
