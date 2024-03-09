import pandas
import scrapy
import json
import logging
from goose3 import Goose
from scrapy.spiders import CrawlSpider, Rule
from scrapy.crawler import CrawlerRunner
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from projectDuck.items import ProjectDuckItem
from twisted.internet import reactor, defer

# from translate import Translator


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
            LinkExtractor(
                restrict_xpaths='//*[@class="mw-content-ltr"]//*[ancestor-or-self::*[@id[contains(.,"subcategories")]]]//*[@href[not(ancestor-or-self::*[@class="reference"]) and not(ancestor-or-self::style)]]'
            ),
            callback="kingdom",
            follow=True,
        ),
        Rule(
            LinkExtractor(
                restrict_xpaths='//*[@class="mw-content-ltr"]//*[not(ancestor-or-self::*[@id[contains(.,"subcategories")]])]//*[@href[not(ancestor-or-self::*[@class="reference"]) and not(ancestor-or-self::style)]]'
            ),
            callback="phylum",
            follow=True,
        ),
    )

    def life(self, response, duck, kingdom=None, phylum=None):

        # domain = response.xpath("/*[@lang]/@lang").get()

        duckling = ProjectDuckItem()

        if kingdom:
            duck[f"{kingdom}"] = response.url

        if phylum:
            duck = dict()
            order = dict()

            for classes in list(
                filter(
                    None,
                    [
                        *[
                            pandas.Series(
                                data.xpath(
                                    './/descendant-or-self::*//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                                ).extract()
                            )
                            .str.cat()
                            .strip()
                            for data in response.xpath(
                                '//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and descendant::th and descendant::td]//th'
                            )
                        ],
                        *[
                            pandas.Series(
                                data.xpath(
                                    './/descendant-or-self::*//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                                ).extract()
                            )
                            .str.cat()
                            .strip()
                            for data in response.xpath(
                                '//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and not(descendant::th) and descendant::td]//td[following-sibling::*]'
                            )
                        ],
                    ],
                )
            ):

                duckling["classes"] = classes

                if "'" not in classes:
                    classes = "'" + classes + "'"
                elif '"' not in classes:
                    classes = '"' + classes + '"'
                else:
                    classes = (
                        "concat('" + classes.replace("'", "',\"'\",'") + "')"
                    )

                if response.xpath(
                    f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and descendant::th and descendant::td]//th[contains(.,{classes})]//br'
                ):
                    duckling["classes"] = (
                        pandas.Series(
                            response.xpath(
                                f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and descendant::th and descendant::td]//th[contains(.,{classes})]//descendant-or-self::*//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                            ).extract()
                        )
                        .str.cat(sep=" ")
                        .strip()
                    )
                if response.xpath(
                    f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and not(descendant::th) and descendant::td]//td[following-sibling::* and contains(.,{classes})]//br'
                ):
                    duckling["classes"] = (
                        pandas.Series(
                            response.xpath(
                                f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and not(descendant::th) and descendant::td]//td[following-sibling::* and contains(.,{classes})]//descendant-or-self::*//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                            ).extract()
                        )
                        .str.cat(sep=" ")
                        .strip()
                    )

                if response.xpath(
                    f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and descendant::th and descendant::td]//th[contains(.,{classes})]'
                ):
                    class_nest = f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and descendant::th and descendant::td]//th[contains(.,{classes})]'
                    order_nest = "//parent::tr//td"

                    duckling["data_url"] = list(
                        filter(
                            None,
                            [
                                *[
                                    data
                                    for data in [
                                        dict(
                                            zip(
                                                [
                                                    pandas.Series(family)
                                                    .str.cat()
                                                    .strip()
                                                    for family in response.xpath(
                                                        f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and @title]//@title'
                                                    ).extract()
                                                ],
                                                [
                                                    response.urljoin(genus)
                                                    for genus in response.xpath(
                                                        f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and @title]//@href'
                                                    ).extract()
                                                ],
                                            )
                                        )
                                    ]
                                ],
                                *[
                                    data
                                    for data in [
                                        dict(
                                            zip(
                                                [
                                                    pandas.Series(family)
                                                    .str.cat()
                                                    .strip()
                                                    for family in response.xpath(
                                                        f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and not(@title)]//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                                                    ).extract()
                                                ],
                                                [
                                                    response.urljoin(genus)
                                                    for genus in response.xpath(
                                                        f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and not(@title)]//@href[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                                                    ).extract()
                                                ],
                                            )
                                        )
                                    ]
                                ],
                            ],
                        )
                    )

                    duckling["data"] = list(
                        filter(
                            None,
                            [
                                pandas.Series(specie).str.cat().strip()
                                for specie in response.xpath(
                                    f'{class_nest}{order_nest}//descendant-or-self::*//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@style="display:none"]) and not(ancestor-or-self::*[@class="reference"]) and not(ancestor-or-self::*[@href])]'
                                ).extract()
                            ],
                        )
                    )

                    order.update(
                        {
                            duckling["classes"]: list(
                                filter(
                                    None,
                                    [*duckling["data_url"], *duckling["data"]],
                                )
                            )
                        }
                    )

                if response.xpath(
                    f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and not(descendant::th) and descendant::td]//td[following-sibling::* and contains(.,{classes})]'
                ):
                    class_nest = f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and not(descendant::th) and descendant::td]//td[following-sibling::* and contains(.,{classes})]'
                    order_nest = "//following-sibling::*"

                    duckling["data_url"] = list(
                        filter(
                            None,
                            [
                                *[
                                    data
                                    for data in [
                                        dict(
                                            zip(
                                                [
                                                    pandas.Series(family)
                                                    .str.cat()
                                                    .strip()
                                                    for family in response.xpath(
                                                        f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and @title]//@title'
                                                    ).extract()
                                                ],
                                                [
                                                    response.urljoin(genus)
                                                    for genus in response.xpath(
                                                        f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and @title]//@href'
                                                    ).extract()
                                                ],
                                            )
                                        )
                                    ]
                                ],
                                *[
                                    data
                                    for data in [
                                        dict(
                                            zip(
                                                [
                                                    pandas.Series(family)
                                                    .str.cat()
                                                    .strip()
                                                    for family in response.xpath(
                                                        f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and not(@title)]//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                                                    ).extract()
                                                ],
                                                [
                                                    response.urljoin(genus)
                                                    for genus in response.xpath(
                                                        f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and not(@title)]//@href[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                                                    ).extract()
                                                ],
                                            )
                                        )
                                    ]
                                ],
                            ],
                        )
                    )

                    duckling["data"] = list(
                        filter(
                            None,
                            [
                                pandas.Series(specie).str.cat().strip()
                                for specie in response.xpath(
                                    f'{class_nest}{order_nest}//descendant-or-self::*//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@style="display:none"]) and not(ancestor-or-self::*[@class="reference"]) and not(ancestor-or-self::*[@href])]'
                                ).extract()
                            ],
                        )
                    )

                    order.update(
                        {
                            duckling["classes"]: list(
                                filter(
                                    None,
                                    [*duckling["data_url"], *duckling["data"]],
                                )
                            )
                        }
                    )

                for data in LinkExtractor(
                    restrict_xpaths=f'{class_nest}{order_nest}//descendant-or-self::*[@href and not(ancestor-or-self::*[@class[contains(.,"new")]])]'
                ).extract_links(response):
                    yield response.follow(
                        data, self.genus, cb_kwargs=dict(family=data.text)
                    )

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
        duckling["url"] = response.url

        duck = dict()
        duck.update({duckling["kingdom"]: duckling["url"]})

        yield from response.follow_all(
            LinkExtractor(
                restrict_xpaths='//*[@class="vector-menu-content-list"]//*[@class="interlanguage-link-target"]//descendant-or-self::*[@href]'
            ).extract_links(response),
            self.life,
            cb_kwargs=dict(duck=duck, kingdom=duckling["kingdom"]),
        )
        yield duck

    def phylum(self, response):

        # domain = response.xpath("/*[@lang]/@lang").get()

        duckling = ProjectDuckItem()
        duckling["phylum"] = response.meta["link_text"]
        duckling["url"] = response.url

        duck = dict()
        order = dict()

        for classes in list(
            filter(
                None,
                [
                    *[
                        pandas.Series(
                            data.xpath(
                                './/descendant-or-self::*//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                            ).extract()
                        )
                        .str.cat()
                        .strip()
                        for data in response.xpath(
                            '//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and descendant::th and descendant::td]//th'
                        )
                    ],
                    *[
                        pandas.Series(
                            data.xpath(
                                './/descendant-or-self::*//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                            ).extract()
                        )
                        .str.cat()
                        .strip()
                        for data in response.xpath(
                            '//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and not(descendant::th) and descendant::td]//td[following-sibling::*]'
                        )
                    ],
                ],
            )
        ):

            duckling["classes"] = classes

            if "'" not in classes:
                classes = "'" + classes + "'"
            elif '"' not in classes:
                classes = '"' + classes + '"'
            else:
                classes = "concat('" + classes.replace("'", "',\"'\",'") + "')"

            if response.xpath(
                f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and descendant::th and descendant::td]//th[contains(.,{classes})]//br'
            ):
                duckling["classes"] = (
                    pandas.Series(
                        response.xpath(
                            f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and descendant::th and descendant::td]//th[contains(.,{classes})]//descendant-or-self::*//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                        ).extract()
                    )
                    .str.cat(sep=" ")
                    .strip()
                )
            if response.xpath(
                f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and not(descendant::th) and descendant::td]//td[following-sibling::* and contains(.,{classes})]//br'
            ):
                duckling["classes"] = (
                    pandas.Series(
                        response.xpath(
                            f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and not(descendant::th) and descendant::td]//td[following-sibling::* and contains(.,{classes})]//descendant-or-self::*//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                        ).extract()
                    )
                    .str.cat(sep=" ")
                    .strip()
                )

            if response.xpath(
                f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and descendant::th and descendant::td]//th[contains(.,{classes})]'
            ):
                class_nest = f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and descendant::th and descendant::td]//th[contains(.,{classes})]'
                order_nest = "//parent::tr//td"

                duckling["data_url"] = list(
                    filter(
                        None,
                        [
                            *[
                                data
                                for data in [
                                    dict(
                                        zip(
                                            [
                                                pandas.Series(family)
                                                .str.cat()
                                                .strip()
                                                for family in response.xpath(
                                                    f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and @title]//@title'
                                                ).extract()
                                            ],
                                            [
                                                response.urljoin(genus)
                                                for genus in response.xpath(
                                                    f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and @title]//@href'
                                                ).extract()
                                            ],
                                        )
                                    )
                                ]
                            ],
                            *[
                                data
                                for data in [
                                    dict(
                                        zip(
                                            [
                                                pandas.Series(family)
                                                .str.cat()
                                                .strip()
                                                for family in response.xpath(
                                                    f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and not(@title)]//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                                                ).extract()
                                            ],
                                            [
                                                response.urljoin(genus)
                                                for genus in response.xpath(
                                                    f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and not(@title)]//@href[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                                                ).extract()
                                            ],
                                        )
                                    )
                                ]
                            ],
                        ],
                    )
                )

                duckling["data"] = list(
                    filter(
                        None,
                        [
                            pandas.Series(specie).str.cat().strip()
                            for specie in response.xpath(
                                f'{class_nest}{order_nest}//descendant-or-self::*//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@style="display:none"]) and not(ancestor-or-self::*[@class="reference"]) and not(ancestor-or-self::*[@href])]'
                            ).extract()
                        ],
                    )
                )

                order.update(
                    {
                        duckling["classes"]: list(
                            filter(
                                None,
                                [*duckling["data_url"], *duckling["data"]],
                            )
                        )
                    }
                )

            if response.xpath(
                f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and not(descendant::th) and descendant::td]//td[following-sibling::* and contains(.,{classes})]'
            ):
                class_nest = f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and not(descendant::th) and descendant::td]//td[following-sibling::* and contains(.,{classes})]'
                order_nest = "//following-sibling::*"

                duckling["data_url"] = list(
                    filter(
                        None,
                        [
                            *[
                                data
                                for data in [
                                    dict(
                                        zip(
                                            [
                                                pandas.Series(family)
                                                .str.cat()
                                                .strip()
                                                for family in response.xpath(
                                                    f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and @title]//@title'
                                                ).extract()
                                            ],
                                            [
                                                response.urljoin(genus)
                                                for genus in response.xpath(
                                                    f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and @title]//@href'
                                                ).extract()
                                            ],
                                        )
                                    )
                                ]
                            ],
                            *[
                                data
                                for data in [
                                    dict(
                                        zip(
                                            [
                                                pandas.Series(family)
                                                .str.cat()
                                                .strip()
                                                for family in response.xpath(
                                                    f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and not(@title)]//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                                                ).extract()
                                            ],
                                            [
                                                response.urljoin(genus)
                                                for genus in response.xpath(
                                                    f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and not(@title)]//@href[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                                                ).extract()
                                            ],
                                        )
                                    )
                                ]
                            ],
                        ],
                    )
                )

                duckling["data"] = list(
                    filter(
                        None,
                        [
                            pandas.Series(specie).str.cat().strip()
                            for specie in response.xpath(
                                f'{class_nest}{order_nest}//descendant-or-self::*//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@style="display:none"]) and not(ancestor-or-self::*[@class="reference"]) and not(ancestor-or-self::*[@href])]'
                            ).extract()
                        ],
                    )
                )

                order.update(
                    {
                        duckling["classes"]: list(
                            filter(
                                None,
                                [*duckling["data_url"], *duckling["data"]],
                            )
                        )
                    }
                )

            for data in LinkExtractor(
                restrict_xpaths=f'{class_nest}{order_nest}//descendant-or-self::*[@href and not(ancestor-or-self::*[@class[contains(.,"new")]])]'
            ).extract_links(response):
                yield response.follow(
                    data, self.genus, cb_kwargs=dict(family=data.text)
                )

        # if order:
        #     duck.update({duckling['phylum']: {duckling['url']: [order, Goose({'keep_footnotes': False}).extract(raw_html=response.body).cleaned_text]}})
        # else:
        #     duck.update({duckling['phylum']: {duckling['url']: Goose({'keep_footnotes': False}).extract(raw_html=response.body).cleaned_text}})

        if order:
            duck.update({duckling["phylum"]: {duckling["url"]: order}})
        else:
            duck.update({duckling["phylum"]: duckling["url"]})

        yield from response.follow_all(
            LinkExtractor(
                restrict_xpaths='//*[@class="vector-menu-content-list"]//*[@class="interlanguage-link-target"]//descendant-or-self::*[@href]'
            ).extract_links(response),
            self.life,
            cb_kwargs=dict(duck=duck, phylum=duckling["phylum"]),
        )
        yield duck

    def genus(self, response, family=None):

        # domain = response.xpath("/*[@lang]/@lang").get()

        duckling = ProjectDuckItem()
        duckling["url"] = response.url

        duck = dict()
        order = dict()

        for classes in list(
            filter(
                None,
                [
                    *[
                        pandas.Series(
                            data.xpath(
                                './/descendant-or-self::*//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                            ).extract()
                        )
                        .str.cat()
                        .strip()
                        for data in response.xpath(
                            '//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and descendant::th and descendant::td]//th'
                        )
                    ],
                    *[
                        pandas.Series(
                            data.xpath(
                                './/descendant-or-self::*//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                            ).extract()
                        )
                        .str.cat()
                        .strip()
                        for data in response.xpath(
                            '//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and not(descendant::th) and descendant::td]//td[following-sibling::*]'
                        )
                    ],
                ],
            )
        ):

            duckling["classes"] = classes

            if "'" not in classes:
                classes = "'" + classes + "'"
            elif '"' not in classes:
                classes = '"' + classes + '"'
            else:
                classes = "concat('" + classes.replace("'", "',\"'\",'") + "')"

            if response.xpath(
                f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and descendant::th and descendant::td]//th[contains(.,{classes})]//br'
            ):
                duckling["classes"] = (
                    pandas.Series(
                        response.xpath(
                            f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and descendant::th and descendant::td]//th[contains(.,{classes})]//descendant-or-self::*//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                        ).extract()
                    )
                    .str.cat(sep=" ")
                    .strip()
                )
            if response.xpath(
                f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and not(descendant::th) and descendant::td]//td[following-sibling::* and contains(.,{classes})]//br'
            ):
                duckling["classes"] = (
                    pandas.Series(
                        response.xpath(
                            f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and not(descendant::th) and descendant::td]//td[following-sibling::* and contains(.,{classes})]//descendant-or-self::*//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                        ).extract()
                    )
                    .str.cat(sep=" ")
                    .strip()
                )

            if response.xpath(
                f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and descendant::th and descendant::td]//th[contains(.,{classes})]'
            ):
                class_nest = f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and descendant::th and descendant::td]//th[contains(.,{classes})]'
                order_nest = "//parent::tr//td"

                duckling["data_url"] = list(
                    filter(
                        None,
                        [
                            *[
                                data
                                for data in [
                                    dict(
                                        zip(
                                            [
                                                pandas.Series(family)
                                                .str.cat()
                                                .strip()
                                                for family in response.xpath(
                                                    f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and @title]//@title'
                                                ).extract()
                                            ],
                                            [
                                                response.urljoin(genus)
                                                for genus in response.xpath(
                                                    f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and @title]//@href'
                                                ).extract()
                                            ],
                                        )
                                    )
                                ]
                            ],
                            *[
                                data
                                for data in [
                                    dict(
                                        zip(
                                            [
                                                pandas.Series(family)
                                                .str.cat()
                                                .strip()
                                                for family in response.xpath(
                                                    f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and not(@title)]//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                                                ).extract()
                                            ],
                                            [
                                                response.urljoin(genus)
                                                for genus in response.xpath(
                                                    f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and not(@title)]//@href[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                                                ).extract()
                                            ],
                                        )
                                    )
                                ]
                            ],
                        ],
                    )
                )

                duckling["data"] = list(
                    filter(
                        None,
                        [
                            pandas.Series(specie).str.cat().strip()
                            for specie in response.xpath(
                                f'{class_nest}{order_nest}//descendant-or-self::*//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@style="display:none"]) and not(ancestor-or-self::*[@class="reference"]) and not(ancestor-or-self::*[@href])]'
                            ).extract()
                        ],
                    )
                )

                order.update(
                    {
                        duckling["classes"]: list(
                            filter(
                                None,
                                [*duckling["data_url"], *duckling["data"]],
                            )
                        )
                    }
                )

            if response.xpath(
                f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and not(descendant::th) and descendant::td]//td[following-sibling::* and contains(.,{classes})]'
            ):
                class_nest = f'//tr[ancestor-or-self::*[@class[contains(.,"infobox") or contains(.,"infocaseta")]] and not(descendant::th) and descendant::td]//td[following-sibling::* and contains(.,{classes})]'
                order_nest = "//following-sibling::*"

                duckling["data_url"] = list(
                    filter(
                        None,
                        [
                            *[
                                data
                                for data in [
                                    dict(
                                        zip(
                                            [
                                                pandas.Series(family)
                                                .str.cat()
                                                .strip()
                                                for family in response.xpath(
                                                    f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and @title]//@title'
                                                ).extract()
                                            ],
                                            [
                                                response.urljoin(genus)
                                                for genus in response.xpath(
                                                    f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and @title]//@href'
                                                ).extract()
                                            ],
                                        )
                                    )
                                ]
                            ],
                            *[
                                data
                                for data in [
                                    dict(
                                        zip(
                                            [
                                                pandas.Series(family)
                                                .str.cat()
                                                .strip()
                                                for family in response.xpath(
                                                    f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and not(@title)]//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                                                ).extract()
                                            ],
                                            [
                                                response.urljoin(genus)
                                                for genus in response.xpath(
                                                    f'{class_nest}{order_nest}//descendant-or-self::*[not(ancestor-or-self::*[@href[contains(.,"wikidata.org")]]) and @href and not(@title)]//@href[not(ancestor-or-self::style) and not(ancestor-or-self::*[@class="reference"])]'
                                                ).extract()
                                            ],
                                        )
                                    )
                                ]
                            ],
                        ],
                    )
                )

                duckling["data"] = list(
                    filter(
                        None,
                        [
                            pandas.Series(specie).str.cat().strip()
                            for specie in response.xpath(
                                f'{class_nest}{order_nest}//descendant-or-self::*//text()[not(ancestor-or-self::style) and not(ancestor-or-self::*[@style="display:none"]) and not(ancestor-or-self::*[@class="reference"]) and not(ancestor-or-self::*[@href])]'
                            ).extract()
                        ],
                    )
                )

                order.update(
                    {
                        duckling["classes"]: list(
                            filter(
                                None,
                                [*duckling["data_url"], *duckling["data"]],
                            )
                        )
                    }
                )

        # if order:
        #     duck.update({family: {duckling['url']: [order, Goose({'keep_footnotes': False}).extract(raw_html=response.body).cleaned_text]}})
        # else:
        #     duck.update({family: {duckling['url']: Goose({'keep_footnotes': False}).extract(raw_html=response.body).cleaned_text}})

        if order:
            duck.update({family: {duckling["url"]: order}})
        else:
            duck.update({family: duckling["url"]})

        yield duck


configure_logging()
runner = CrawlerRunner(get_project_settings())


@defer.inlineCallbacks
def crawler():
    yield runner.crawl(DuckSpider)
    reactor.stop()


crawler()
reactor.run()
