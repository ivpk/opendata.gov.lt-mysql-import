import collections


class FakeCache(object):

    def __init__(self):
        self.data = collections.defaultdict(list)

    def __contains__(self, web_and_url):
        website = web_and_url['website']
        url = web_and_url['url']
        return self.data.get(website, {}).get('url') == url

    def update(self, item):
        self.data[item['website']].append(item)

    def get_url_data(self, website):
        for item in self.data.get(website, []):
            if item['is_data']:
                yield item
