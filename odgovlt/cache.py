import sqlalchemy as sa
import datetime
import logging

logging.info('Starting logger for cache')
log = logging.getLogger(__name__)


class Cache(object):

    def __init__(self, db):
        self.conn = sa.create_engine(db)
        self.conn.echo = False
        self.meta = sa.MetaData(self.conn)
        self.meta.reflect(bind=self.conn)

        if 'data' not in self.meta.tables:
            self.data = sa.Table(
                'data', self.meta,
                sa.Column('id', sa.Integer, primary_key=True),
                sa.Column('website', sa.String(200)),
                sa.Column('url', sa.String(200)),
                sa.Column('name', sa.String(200)),
                sa.Column('date_accessed', sa.DateTime),
                sa.Column('is_data', sa.Boolean),
                sa.Column('type', sa.String(100)),
                sa.Column('cached_forever', sa.Boolean),
            )
            self.data.create()
        else:
            self.data = self.meta.tables['data']

    def __contains__(self, web_and_url):
        website = web_and_url['website']
        url = web_and_url['url']
        try:
            clause = self.data.select().where(self.data.c.website == website).where(self.data.c.url == url)
        except sa.exc.ProgrammingError as e:
            log.error(e)
            return True
        for row in self.conn.execute(clause):
            return True
        return False

    def __reset__(self):
        self.time_now = datetime.datetime.now()
        remove = self.data.delete().where(self.time_now - self.data.c.date_accessed == 0)
        self.res = self.conn.execute(remove)
        self.res.close()

    def update(self, web_and_url):
        new_website = web_and_url['website']
        new_url = web_and_url['url']
        new_name = web_and_url['name']
        url_type = web_and_url['type']
        if not self.__contains__({'website': new_website, 'url': new_url}):
            try:
                new_is_data = web_and_url['is_data']
            except KeyError as e:
                log.error(e)
                new_is_data = False
            try:
                new_cached_forever = web_and_url['cached_forever']
            except KeyError as e:
                log.error(e)
                new_cached_forever = False
            update = self.data.insert()
            try:
                self.res = update.execute(
                    website=new_website,
                    url=new_url,
                    name=new_name,
                    date_accessed=datetime.datetime.now(),
                    is_data=new_is_data,
                    type=url_type,
                    cached_forever=new_cached_forever)
                self.res.close()
            except sa.exc.ProgrammingError as e:
                log.error(e)

    def remove_old(self, days=30):
        self.time_now = datetime.datetime.now()
        remove = self.data.delete().where(self.time_now - self.data.c.date_accessed == days).where(not self.data.c.cached_forever)
        self.res = self.conn.execute(remove)
        self.res.close()

    def get_url_data(self, website):
        data_list = []
        clause = self.data.select().where(self.data.c.website == website).where(self.data.c.is_data)
        for row in self.conn.execute(clause):
            data_list.append(dict(row))
        return data_list

    def get_all(self):
        clause = self.data.select()
        data_to_return = []
        for row in self.conn.execute(clause):
            data_to_return.append(dict(row))
        return data_to_return

    def get_all_data(self):
        data_list = []
        clause = self.data.select().where(self.data.c.is_data)
        for row in self.conn.execute(clause):
            data_list.append(dict(row))
        return data_list
