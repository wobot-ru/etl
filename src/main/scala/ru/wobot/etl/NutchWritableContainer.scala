package ru.wobot.etl

import org.apache.nutch.crawl.CrawlDatum
import org.apache.nutch.parse.{ParseData, ParseText}

class NutchWritableContainer(var fetchDatum: CrawlDatum = null, var parseText: ParseText = null, var parseData: ParseData = null) {

}
