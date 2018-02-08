import os

import sqlalchemy as sa
from cache.cache import Cache


def test_cache():
    path = 'sqlite:///%s/test_cache.db' % os.path.dirname(os.path.abspath(__file__))
    cache = Cache(path)
    cache.__reset__()
    engine = sa.create_engine(path)
    metadata = sa.MetaData(engine)
    metadata.reflect(bind=engine)
    data = metadata.tables['data']
    clause = data.select()
    database_keys = []
    for row in engine.execute(clause).keys():
        database_keys.append(row)
    assert database_keys == ['id', 'website', 'url', 'name', 'date_accessed',
                             'is_data', 'type', 'cached_forever']
    cache_dict = {'website': 'test1', 'url': 'test2', 'name': 'test3',
                  'type': 'test4', 'cached_forever': True, 'is_data': False}
    cache_dict2 = {'website': 'test5', 'url': 'test6', 'name': 'test7',
                   'type': 'test8', 'cached_forever': False, 'is_data': True}
    cache_dict3 = {'website': 'test5', 'url': 'test2', 'name': 'test3',
                   'type': 'test4', 'cached_forever': True, 'is_data': False}
    cache.update(cache_dict)
    cache.update(cache_dict2)
    cache.update(cache_dict3)
    database_data = []
    for row in engine.execute(clause):
        database_data.append({
            'website': row.website, 'url': row.url, 'name': row.name,
            'type': row.type, 'cached_forever': row.cached_forever,
            'is_data': row.is_data})
    assert database_data[0] == cache_dict
    assert database_data[1] == cache_dict2
    assert database_data[2] == cache_dict3
    assert cache.__contains__({'website': 'test1', 'url': 'test2'})
    assert cache.__contains__({'website': 'test5', 'url': 'test6'})
    assert cache.__contains__({'website': 'test5', 'url': 'test2'})
    assert cache.get_url_data('test1') == []
    test5_dict = cache.get_url_data('test5')
    assert isinstance(test5_dict, list)
    assert len(test5_dict) == 1
    assert test5_dict[0]['id'] == 2
    assert test5_dict[0]['website'] == cache_dict2['website']
    assert test5_dict[0]['url'] == cache_dict2['url']
    assert test5_dict[0]['name'] == cache_dict2['name']
    assert test5_dict[0]['type'] == cache_dict2['type']
    assert test5_dict[0]['cached_forever'] == cache_dict2['cached_forever']
    assert test5_dict[0]['is_data'] == cache_dict2['is_data']
    all_dict = cache.get_all()
    assert isinstance(all_dict, list)
    assert len(all_dict) == 3
    assert all_dict[0]['id'] == 1
    assert all_dict[0]['website'] == cache_dict['website']
    assert all_dict[0]['url'] == cache_dict['url']
    assert all_dict[0]['name'] == cache_dict['name']
    assert all_dict[0]['type'] == cache_dict['type']
    assert all_dict[0]['cached_forever'] == cache_dict['cached_forever']
    assert all_dict[0]['is_data'] == cache_dict['is_data']
    assert all_dict[1]['id'] == 2
    assert all_dict[1]['website'] == cache_dict2['website']
    assert all_dict[1]['url'] == cache_dict2['url']
    assert all_dict[1]['name'] == cache_dict2['name']
    assert all_dict[1]['type'] == cache_dict2['type']
    assert all_dict[1]['cached_forever'] == cache_dict2['cached_forever']
    assert all_dict[1]['is_data'] == cache_dict2['is_data']
    assert all_dict[2]['id'] == 3
    assert all_dict[2]['website'] == cache_dict3['website']
    assert all_dict[2]['url'] == cache_dict3['url']
    assert all_dict[2]['name'] == cache_dict3['name']
    assert all_dict[2]['type'] == cache_dict3['type']
    assert all_dict[2]['cached_forever'] == cache_dict3['cached_forever']
    assert all_dict[2]['is_data'] == cache_dict3['is_data']
    all_data = cache.get_all_data()
    assert isinstance(all_data, list)
    assert len(all_data) == 1
    assert all_data[0]['id'] == 2
    assert all_data[0]['website'] == cache_dict2['website']
    assert all_data[0]['url'] == cache_dict2['url']
    assert all_data[0]['name'] == cache_dict2['name']
    assert all_data[0]['type'] == cache_dict2['type']
    assert all_data[0]['cached_forever'] == cache_dict2['cached_forever']
    assert all_data[0]['is_data'] == cache_dict2['is_data']
