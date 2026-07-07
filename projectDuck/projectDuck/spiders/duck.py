import pandas
import scrapy
import json
import logging
import sys
from goose3 import Goose
from scrapy.spiders import CrawlSpider, Rule

# from scrapy.crawler import CrawlerRunner
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.log import configure_logging

from scrapy.utils.project import get_project_settings
from projectDuck.items import ProjectDuckItem

# from translate import Translator

from scrapy.crawler import AsyncCrawlerRunner

from scrapy.utils.defer import deferred_f_from_coro_f
from scrapy.utils.reactor import install_reactor
from twisted.internet.task import react


class DuckSpider(CrawlSpider):
    name = "duck"
    allowed_domains = [r"wikipedia.org"]
    start_urls = [
        r"https://en.wikipedia.org/wiki/Category:Works_about_animals"
    ]

    custom_settings = {
        "FEEDS": {"duck.json": {"format": "jsonlines", "overwrite": True}}
    }

    logging.basicConfig(
        filename="duck.log",
        filemode="w+",
        encoding="utf-8",
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.NOTSET,
        force=True,
    )

    rules = (
        Rule(
            LinkExtractor(restrict_css="#mw-subcategories ul a[href]"),
            callback="kingdom",
            follow=True,
        ),
        Rule(
            LinkExtractor(restrict_css="#mw-pages ul a[href]"),
            callback="phylum",
            follow=True,
        ),
    )

    def life(self, response, order, duckling=ProjectDuckItem(), family=None):

        if family:
            genus = response.url
            duck = dict()
            order = dict()

        for classes_data in list(
            filter(
                None,
                [
                    *[
                        pandas.Series(
                            data.xpath(
                                './descendant-or-self::*/text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                            ).extract()
                        )
                        .str.cat()
                        .strip()
                        for data in response.xpath(
                            '//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(descendant-or-self::*[@class[contains(.,"entete")]]) and descendant::th and descendant::td]/th'
                        )
                    ],
                    *[
                        pandas.Series(
                            data.xpath(
                                './descendant-or-self::*/text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                            ).extract()
                        )
                        .str.cat()
                        .strip()
                        for data in response.xpath(
                            '//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(descendant-or-self::*[@class[contains(.,"entete")]]) and descendant::th and descendant::td]/../tr/td[following-sibling::td and not(ancestor::td) and descendant-or-self::*[@href or text()]]'
                        )
                    ],
                    *[
                        pandas.Series(
                            data.xpath(
                                './descendant-or-self::*/text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                            ).extract()
                        )
                        .str.cat()
                        .strip()
                        for data in response.xpath(
                            '//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(descendant-or-self::*[@class[contains(.,"entete")]]) and not(descendant::th) and descendant::td]/td[following-sibling::td and not(ancestor::td) and descendant-or-self::*[@href or text()]]'
                        )
                    ],
                ],
            )
        ):

            classes_data = self.classes(response, duckling, classes_data)

            if response.xpath(
                f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(descendant-or-self::*[@class[contains(.,"entete")]]) and descendant::th and descendant::td]/th[contains(.,{classes_data})]'
            ):
                class_nest = f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(descendant-or-self::*[@class[contains(.,"entete")]]) and descendant::th and descendant::td]/th[contains(.,{classes_data})]'
                order_nest = "/parent::tr/td"

                if response.xpath(
                    f"{class_nest}/parent::tr[not(ancestor::tr)]"
                ):
                    # Code for XPath having not(ancestor::tr)
                    self.order(
                        response, class_nest, order_nest, order, duckling
                    )
                    if not family:
                        # Changes for if not family block for not(ancestor::tr) condition
                        for genus in LinkExtractor(
                            restrict_xpaths=f'{class_nest}{order_nest}/descendant-or-self::*[@href and not(ancestor-or-self::*[@class[contains(.,"new")]])]'
                        ).extract_links(response):
                            yield response.follow(
                                genus,
                                self.life,
                                cb_kwargs=dict(order=order, family=genus.text),
                            )
                if response.xpath(f"{class_nest}/parent::tr[ancestor::tr]"):
                    # Code for XPath having ancestor::tr
                    self.order(
                        response, class_nest, order_nest, order, duckling
                    )
                    if not family:
                        # Changes for if not family block for ancestor::tr condition
                        for genus in LinkExtractor(
                            restrict_xpaths=f'{class_nest}{order_nest}/descendant-or-self::*[@href and not(ancestor-or-self::*[@class[contains(.,"new")]])]'
                        ).extract_links(response):
                            yield response.follow(
                                genus,
                                self.life,
                                cb_kwargs=dict(order=order, family=genus.text),
                            )

            if response.xpath(
                f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(descendant-or-self::*[@class[contains(.,"entete")]]) and descendant::th and descendant::td]/../tr/td[following-sibling::td and not(ancestor::td) and descendant-or-self::*[@href or text()] and contains(.,{classes_data})]'
            ):
                class_nest = f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(descendant-or-self::*[@class[contains(.,"entete")]]) and descendant::th and descendant::td]/../tr/td[following-sibling::td and not(ancestor::td) and descendant-or-self::*[@href or text()] and contains(.,{classes_data})]'
                order_nest = "/following-sibling::td"

                self.order(response, class_nest, order_nest, order, duckling)
                if not family:
                    for genus in LinkExtractor(
                        restrict_xpaths=f'{class_nest}{order_nest}/descendant-or-self::*[@href and not(ancestor-or-self::*[@class[contains(.,"new")]])]'
                    ).extract_links(response):
                        yield response.follow(
                            genus,
                            self.life,
                            cb_kwargs=dict(order=order, family=genus.text),
                        )

            if response.xpath(
                f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(descendant-or-self::*[@class[contains(.,"entete")]]) and not(descendant::th) and descendant::td]/td[following-sibling::td and not(ancestor::td) and descendant-or-self::*[@href or text()] and contains(.,{classes_data})]'
            ):
                class_nest = f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(descendant-or-self::*[@class[contains(.,"entete")]]) and not(descendant::th) and descendant::td]/td[following-sibling::td and not(ancestor::td) and descendant-or-self::*[@href or text()] and contains(.,{classes_data})]'
                order_nest = "/following-sibling::td"

                self.order(response, class_nest, order_nest, order, duckling)
                if not family:
                    for genus in LinkExtractor(
                        restrict_xpaths=f'{class_nest}{order_nest}/descendant-or-self::*[@href and not(ancestor-or-self::*[@class[contains(.,"new")]])]'
                    ).extract_links(response):
                        yield response.follow(
                            genus,
                            self.life,
                            cb_kwargs=dict(order=order, family=genus.text),
                        )

        for order_data in list(
            filter(
                None,
                [
                    pandas.Series(
                        data.xpath(
                            './descendant-or-self::*/text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                        ).extract()
                    )
                    .str.cat()
                    .strip()
                    for data in response.xpath(
                        '//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(ancestor-or-self::*[@class[contains(.,"wikitable")]] or descendant-or-self::*[@class[contains(.,"entete") or contains(.,"navigation-only")] or @scope or th or hr])]/td[not(ancestor::td) and descendant-or-self::td[@href or text()]]'
                    )
                ],
            )
        ):
            if "'" not in order_data:
                order_data = "'" + order_data + "'"
            elif '"' not in order_data:
                order_data = '"' + order_data + '"'
            else:
                order_data = (
                    "concat('" + order_data.replace("'", "',\"'\",'") + "')"
                )

            class_nest = None
            order_nest = f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(ancestor-or-self::*[@class[contains(.,"wikitable")]] or descendant-or-self::*[@class[contains(.,"entete") or contains(.,"navigation-only")] or @scope or th or hr])]/td[not(ancestor::td) and descendant-or-self::td[@href or text()] and contains(.,{order_data})]'

            self.order(response, class_nest, order_nest, order, duckling)
            if not family:
                for genus in LinkExtractor(
                    restrict_xpaths=f'{order_nest}/descendant-or-self::*[@href and not(ancestor-or-self::*[@class[contains(.,"new")]])]'
                ).extract_links(response):
                    yield response.follow(
                        genus,
                        self.life,
                        cb_kwargs=dict(order=order, family=genus.text),
                    )

        if family:
            # if order:
            #     duck.update({family: {genus: [order, Goose({'keep_footnotes': False}).extract(raw_html=response.body).cleaned_text]}})
            # else:
            #     duck.update({family: {genus: Goose({'keep_footnotes': False}).extract(raw_html=response.body).cleaned_text}})

            if order:
                duck.update({family: {genus: order}})
            else:
                duck.update({family: genus})

            yield duck

    def domain(self, response, kingdom=None, phylum=None):

        # response.xpath("/*[@lang]/@lang").get()

        duck = dict()

        if kingdom:
            duck[f"{kingdom}"] = response.url

        if phylum:
            order = dict()

            yield from self.life(response, order)

            # if order:
            #     duck[f"{phylum}"] = {response.url: [order, Goose({'keep_footnotes': False}).extract(raw_html=response.body).cleaned_text]}
            # else:
            #     duck[f"{phylum}"] = {response.url: Goose({'keep_footnotes': False}).extract(raw_html=response.body).cleaned_text}

            if order:
                duck[f"{phylum}"] = {response.url: order}
            else:
                duck[f"{phylum}"] = response.url

        yield duck

    def kingdom(self, response):

        duckling = ProjectDuckItem()
        duckling["kingdom"] = response.meta["link_text"]

        duck = dict()
        duck.update({duckling["kingdom"]: response.url})

        yield from response.follow_all(
            LinkExtractor(
                restrict_xpaths='//*[@class="vector-menu-content-list"]//*[@class="interlanguage-link-target"]//descendant-or-self::*[@href]'
            ).extract_links(response),
            self.domain,
            cb_kwargs=dict(kingdom=duckling["kingdom"]),
        )
        yield duck

    def phylum(self, response):

        # response.xpath("/*[@lang]/@lang").get()

        duckling = ProjectDuckItem()
        duckling["phylum"] = response.meta["link_text"]

        duck = dict()
        order = dict()

        yield from self.life(response, order)

        # if order:
        #     duck.update({duckling['phylum']: {response.url: [order, Goose({'keep_footnotes': False}).extract(raw_html=response.body).cleaned_text]}})
        # else:
        #     duck.update({duckling['phylum']: {response.url: Goose({'keep_footnotes': False}).extract(raw_html=response.body).cleaned_text}})

        if order:
            duck.update({duckling["phylum"]: {response.url: order}})
        else:
            duck.update({duckling["phylum"]: response.url})

        yield from response.follow_all(
            LinkExtractor(
                restrict_xpaths='//*[@class="vector-menu-content-list"]//*[@class="interlanguage-link-target"]//descendant-or-self::*[@href]'
            ).extract_links(response),
            self.domain,
            cb_kwargs=dict(phylum=duckling["phylum"]),
        )
        yield duck

    def classes(self, response, duckling, classes_data):
        duckling["classes"] = classes_data

        if "'" not in classes_data:
            classes_data = "'" + classes_data + "'"
        elif '"' not in classes_data:
            classes_data = '"' + classes_data + '"'
        else:
            classes_data = (
                "concat('" + classes_data.replace("'", "',\"'\",'") + "')"
            )

        # Apply these if-statements for br and wbr nodes in order()

        # br
        if response.xpath(
            f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(descendant-or-self::*[@class[contains(.,"entete")]]) and descendant::th and descendant::td]/th[contains(.,{classes_data})]//br'
        ):
            duckling["classes"] = (
                pandas.Series(
                    response.xpath(
                        f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(descendant-or-self::*[@class[contains(.,"entete")]]) and descendant::th and descendant::td]/th[contains(.,{classes_data})]/descendant-or-self::*/text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                    ).extract()
                )
                .str.cat(sep=" ")
                .strip()
            )

        if response.xpath(
            f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(descendant-or-self::*[@class[contains(.,"entete")]]) and descendant::th and descendant::td]/../tr/td[following-sibling::td and not(ancestor::td) and descendant-or-self::*[@href or text()] and contains(.,{classes_data})]//br'
        ):
            duckling["classes"] = (
                pandas.Series(
                    response.xpath(
                        f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(descendant-or-self::*[@class[contains(.,"entete")]]) and descendant::th and descendant::td]/../tr/td[following-sibling::td and not(ancestor::td) and descendant-or-self::*[@href or text()] and contains(.,{classes_data})]/descendant-or-self::*/text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                    ).extract()
                )
                .str.cat(sep=" ")
                .strip()
            )

        if response.xpath(
            f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(descendant-or-self::*[@class[contains(.,"entete")]]) and not(descendant::th) and descendant::td]/td[following-sibling::td and not(ancestor::td) and descendant-or-self::*[@href or text()] and contains(.,{classes_data})]//br'
        ):
            duckling["classes"] = (
                pandas.Series(
                    response.xpath(
                        f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(descendant-or-self::*[@class[contains(.,"entete")]]) and not(descendant::th) and descendant::td]/td[following-sibling::td and not(ancestor::td) and descendant-or-self::*[@href or text()] and contains(.,{classes_data})]/descendant-or-self::*/text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                    ).extract()
                )
                .str.cat(sep=" ")
                .strip()
            )

        # wbr
        if response.xpath(
            f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(descendant-or-self::*[@class[contains(.,"entete")]]) and descendant::th and descendant::td]/th[contains(.,{classes_data})]//wbr'
        ):
            duckling["classes"] = (
                pandas.Series(
                    response.xpath(
                        f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(descendant-or-self::*[@class[contains(.,"entete")]]) and descendant::th and descendant::td]/th[contains(.,{classes_data})]/descendant-or-self::*/text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                    ).extract()
                )
                .str.cat(sep=" ")
                .strip()
            )

        if response.xpath(
            f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(descendant-or-self::*[@class[contains(.,"entete")]]) and descendant::th and descendant::td]/../tr/td[following-sibling::td and not(ancestor::td) and descendant-or-self::*[@href or text()] and contains(.,{classes_data})]//wbr'
        ):
            duckling["classes"] = (
                pandas.Series(
                    response.xpath(
                        f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(descendant-or-self::*[@class[contains(.,"entete")]]) and descendant::th and descendant::td]/../tr/td[following-sibling::td and not(ancestor::td) and descendant-or-self::*[@href or text()] and contains(.,{classes_data})]/descendant-or-self::*/text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                    ).extract()
                )
                .str.cat(sep=" ")
                .strip()
            )

        if response.xpath(
            f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(descendant-or-self::*[@class[contains(.,"entete")]]) and not(descendant::th) and descendant::td]/td[following-sibling::td and not(ancestor::td) and descendant-or-self::*[@href or text()] and contains(.,{classes_data})]//wbr'
        ):
            duckling["classes"] = (
                pandas.Series(
                    response.xpath(
                        f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta") or contains(.,"hiddenStructure")]] and not(descendant-or-self::*[@class[contains(.,"entete")]]) and not(descendant::th) and descendant::td]/td[following-sibling::td and not(ancestor::td) and descendant-or-self::*[@href or text()] and contains(.,{classes_data})]/descendant-or-self::*/text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                    ).extract()
                )
                .str.cat(sep=" ")
                .strip()
            )

        return classes_data

    def order(self, response, class_nest, order_nest, order, duckling):
        self.family(response, class_nest, order_nest, duckling)
        self.genus(response, class_nest, order_nest, duckling)
        self.specie(response, class_nest, order_nest, duckling)

        if class_nest:
            order.update(
                {
                    duckling["classes"]: list(
                        filter(
                            None,
                            [
                                *list(
                                    filter(
                                        None,
                                        [
                                            data
                                            for data in [
                                                dict(
                                                    zip(
                                                        duckling["family"],
                                                        duckling["genus"],
                                                    )
                                                )
                                            ]
                                        ],
                                    )
                                ),
                                *duckling["specie"],
                            ],
                        )
                    )
                }
            )
        else:
            pass

    def family(self, response, class_nest, order_nest, duckling):
        duckling["family"] = [
            *[
                pandas.Series(family).str.cat().strip()
                for family in response.xpath(
                    f'{class_nest or ""}{order_nest}/descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and @title]/@title'
                ).extract()
            ],
            *[
                pandas.Series(family).str.cat().strip()
                for family in response.xpath(
                    f'{class_nest or ""}{order_nest}/descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and not(@title) and descendant-or-self::text()]/text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                ).extract()
            ],
            *[
                pandas.Series(family).str.cat().strip()
                for family in response.xpath(
                    f'{class_nest or ""}{order_nest}/descendant-or-self::*[@href[contains(.,"wikidata.org")] and descendant-or-self::text()]/text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                ).extract()
            ],
        ]

    def genus(self, response, class_nest, order_nest, duckling):
        duckling["genus"] = [
            *[
                response.urljoin(genus)
                for genus in response.xpath(
                    f'{class_nest or ""}{order_nest}/descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and @title]/@href'
                ).extract()
            ],
            *[
                response.urljoin(genus)
                for genus in response.xpath(
                    f'{class_nest or ""}{order_nest}/descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and not(@title) and descendant-or-self::text()]/@href[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                ).extract()
            ],
            *[
                response.urljoin(genus)
                for genus in response.xpath(
                    f'{class_nest or ""}{order_nest}/descendant-or-self::*[@href[contains(.,"wikidata.org")] and descendant-or-self::text()]/@href[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                ).extract()
            ],
        ]

    def specie(self, response, class_nest, order_nest, duckling):
        duckling["specie"] = list(
            filter(
                None,
                [
                    pandas.Series(specie).str.cat().strip()
                    for specie in response.xpath(
                        f'{class_nest or ""}{order_nest}/descendant-or-self::*[not(ancestor-or-self::style) and not(ancestor-or-self::*[@style="display:none"]) and not(ancestor-or-self::*[@class="reference"]) and not(ancestor-or-self::*[@href])]/text()'
                    ).extract()
                ],
            )
        )


async def crawl(_):
    configure_logging({"LOG_FORMAT": "%(levelname)s: %(message)s"})
    runner = AsyncCrawlerRunner(get_project_settings())
    runner.crawl(DuckSpider)
    await runner.join()  # completes when both spiders finish


if __name__ == "__main__":
    install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")
    react(deferred_f_from_coro_f(crawl))
