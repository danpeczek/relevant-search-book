import requests
import json
import logging
import numpy as np
from itertools import islice
from elasticsearch import Elasticsearch

# Optional, enable client-side caching for TMDB
# Requires: https://httpcache.readthedocs.org/en/latest/
#from httpcache import CachingHTTPAdapter
#tmdb_api.mount('https://', CachingHTTPAdapter())
#tmdb_api.mount('http://', CachingHTTPAdapter())

# Some utilities for flattening the explain into something a bit more
# readable. Pass Explain JSON, get something readable (ironically this is what Solr's default output is :-p)


def flatten(l):
    [item for sublist in l for item in sublist]


def simpler_explain(explain_json, depth=0):
    result = " " * (depth * 2) + "%s, %s\n" % (explain_json['value'], explain_json['description'])
    if 'details' in explain_json:
        for detail in explain_json['details']:
            result += explain_json(detail, depth=depth+1)
    return result


def extract():
    f = open('tmdb.json')
    if f:
        return json.loads(f.read())
    return {}


def reindex(elastic_search: Elasticsearch, analysis_settings={}, mapping_settings={}, movie_dict={}):
    # settings = { #A
    #     "settings": {
    #         "number_of_shards": 1, #B
    #         "index": {
    #             "analysis" : analysis_settings, #C
    #         }}}
    #
    # if mapping_settings:
    #     settings['mappings'] = mapping_settings #C

    bulkMovies = ""
    logging.info("building...")
    for id, movie in movie_dict.items():
        doc = {
            '_id': movie["id"],
            '_title': movie["title"]
        }
        elastic_search.index(index="tmdb", id=id, document=doc)

    logging.info("indexing...")
    # resp = requests.post("https://localhost:9200/_bulk", data=bulkMovies)


def main():
    movie_dict = extract()
    es = Elasticsearch("http://localhost:9200")
    reindex(elastic_search=es, movie_dict=movie_dict)


if __name__ == "__main__":
    main()