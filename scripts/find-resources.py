from __future__ import unicode_literals, print_function

import argparse
import logging
import collections
import ConfigParser as configparser
import functools

import sqlalchemy as sa
import tqdm

from odgovlt import get_web
from odgovlt.testing import FakeCache
from odgovlt.cache import Cache


def main():
    level = logging.WARNING
    logging.root.setLevel(level)
    logging.root.disabled = False
    logging.basicConfig(level=level)

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', required=True, help="path conf ckan ini config file")
    parser.add_argument('-u', '--url', help="visit specified url")
    parser.add_argument('--no-cache', action='store_true', help="disable cache")
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.config)

    engine = sa.create_engine(config.get('app:main', 'odgovltimport.externaldb.url'))

    meta = sa.MetaData(bind=engine)
    meta.reflect()
    tables = {
        'user': meta.tables['t_user'],
        'istaiga': meta.tables['t_istaiga'],
        'rinkmena': meta.tables['t_rinkmena'],
        'kategorija': meta.tables['t_kategorija'],
        'kategorija_rinkmena': meta.tables['t_kategorija_rinkmena'],
    }
    t = collections.namedtuple('Tables', tables.keys())(**tables)

    if args.no_cache:
        cache = FakeCache()
    else:
        cache = Cache('sqlite:///%s/cache.db' % config.get('app:main', 'cache_dir'))

    if args.url:
        urls = [args.url]
    else:
        query = (
            sa.select([t.rinkmena]).
            where(t.rinkmena.c.STATUSAS == 'U')
        )
        urls = [row['TINKLAPIS'] for row in engine.execute(query)]

    for url in urls:
        print(url)

        for resource in get_web(cache, url, progressbar=functools.partial(tqdm.tqdm, leave=False)):
            cache.update(resource)

        for resource in cache.get_url_data(url):
            print('  %-10s  %-40s  %s' % (resource['type'], resource['name'], resource['url']))


if __name__ == "__main__":
    main()
