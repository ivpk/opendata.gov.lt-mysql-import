# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from sqlalchemy import create_engine
from sqlalchemy import MetaData
import sqlalchemy as sa
from ckanext.harvest.model import HarvestObject
import logging
from ckanext.harvest.harvesters.base import HarvesterBase
import json
from datetime import datetime
from ckan.logic import get_action
from ckan.plugins import toolkit
from ckan import model
import re
import string
import unidecode
import itertools

log = logging.getLogger(__name__)


def fixcase(value):
    if len(value) > 1 and \
           value[:2].isalpha() and \
           value[0].isupper() and \
           value[1].islower():
        return value[0].lower() + value[1:]
    else:
        return value


def slugify(title=None, length=90):
    if not title:
        return ''

    # Replace all non-ascii characters to ascii equivalents.
    slug = unidecode.unidecode(title)

    # Make slug.
    slug = str(re.sub(r'[^\w\s-]', '', slug).strip().lower())
    slug = re.sub(r'[-\s]+', '-', slug)

    # Make sure, that slug is not longer that specied in `length`.
    if len(slug) > length:
        left = []
        right = []
        words = slug.split('-')
        split = int(len(words) * .6)
        index = itertools.izip_longest(
            ((i, left) for i in range(split)),
            ((i, right) for i in range(len(words) - 1, split - 1, -1)),
        )
        index = (i for i in
                 itertools.chain.from_iterable(index) if i is not None)
        total = 0
        for k, (i, q) in zip(itertools.chain([0], itertools.count(2)), index):
            if total + len(words[i]) + k > length:
                break
            else:
                q.append(words[i])
                total += len(words[i])
        slug = '-'.join(left) + '--' + '-'.join(right)

    return slug


def tagify(tag):
    spl = re.split(r'\W+', tag, flags=re.UNICODE)
    return ' '.join(spl).strip()


def get_package_tags(r_zodziai):
    name_list = []
    if r_zodziai:
        tags = map(fixcase,
                   map(string.strip,
                       r_zodziai.replace(';', ',').split(',')))
        for tag in filter(None, tags):
            name = tagify(tag).lower()

            if len(name) > 100:
                log.warn("skip very long tag: %r", tag)
            else:
                name_list.append(name)
    return name_list


class CkanAPI(object):
    """Wrapper around CKAN API actions.
    See: http://docs.ckan.org/en/latest/api/index.html#action-api-reference
    """

    def __getattr__(self, name):
        return lambda context={}, \
                      **kwargs: toolkit.get_action(name)(context, kwargs)


class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            try:
                return obj.strftime('%Y-%m-%dT%H:%M:%S')
            except ValueError:
                # strftime gives ValueError for 0000-00-00 00:00:00 datetimes.
                return None
        else:
            return super(DatetimeEncoder, obj).default(obj)


class OdgovltHarvester(HarvesterBase):

    def _connect_to_database(self, dburl):
        con = create_engine(dburl)
        meta = MetaData(bind=con, reflect=True)
        return con, meta

    def sync_importbot_user(self):
        from ckan import model

        username = 'importbot'
        user = model.User.by_name(username)

        if not user:
            # TODO: How to deal with passwords?
            user = model.User(name=username, password='secret123')
            user.sysadmin = True
            model.Session.add(user)
            model.repo.commit_and_remove()

        return self.api.user_show(id=username)

    def sync_user(self, user_id, conn):
        self.api = CkanAPI()
        self.importbot = self.sync_importbot_user()
        user = conn.execute(
            sa.select([self.t.user]).where(self.t.user.c.ID == user_id)
        ).fetchone()
        if user:
            user_data = {
                'name': slugify(user.LOGIN),
                # TODO: Passwrods are encoded with md5 hash, I need to look if
                #       CKAN supports md5 hashed passwords. If not,
                #       then users will have to change their passwords.
                'email': user.EMAIL,
                'password': user.PASS,
                'fullname': ' '.join([user.FIRST_NAME, user.LAST_NAME]),
            }
        else:
            user_data = {
                'name': 'unknown',
                # TODO: What email should I use?
                'email': 'unknown@example.com',
                # TODO: Maybe dynamically generate a password?
                'password': 'secret123',
                'fullname': 'Unknown User',
            }

        ckan_user = next((
            u for u in self.api.user_list(q=user_data['name'])
            if u['name'] == user_data['name']
        ), None)

        if ckan_user is None:
            context = {'user': self.importbot['name']}
            ckan_user = self.api.user_create(context, **user_data)

        user_data['id'] = ckan_user['id']

        return user_data

    def info(self):
        return {
            'name': 'opendata-gov-lt',
            'title': 'opendata.gov.lt',
            'description': 'Harvest opendata.gov.lt',
            'form_config_interface': 'Text'
        }

    def gather_stage(self, harvest_job):
        log.debug('In opendatagov gather_stage')
        con, meta = self._connect_to_database(harvest_job.source.url)

        class Tables(object):
            user = meta.tables['t_user']
            istaiga = meta.tables['t_istaiga']
            rinkmena = meta.tables['t_rinkmena']

        self.t = Tables

        ids = []
        database_data = {}
        query = (
            sa.select([self.t.rinkmena]).
            where(self.t.rinkmena.c.STATUSAS == 'U')
        )
        for row in con.execute(query):
            database_data = dict(row)
            id = database_data['ID']
            conn = con.connect()
            user = self.sync_user(row.USER_ID, conn)
            database_data['USER_NAME'] = user['fullname']
            obj = HarvestObject(
                         guid=id,
                         job=harvest_job,
                         content=json.dumps(
                              database_data,
                              cls=DatetimeEncoder))
            obj.save()
            ids.append(obj.id)
        return ids

    def fetch_stage(self, harvest_object):
        return True

    def import_stage(self, harvest_object):
        base_context = {'model': model, 'session': model.Session,
                        'user': self._get_user_name()}
        data_to_import = json.loads(harvest_object.content)
        source_dataset = get_action('package_show')(
                           base_context.copy(),
                           {'id': harvest_object.source.id})
        local_org = source_dataset.get('owner_org')
        package_dict = {
            'id': harvest_object.guid,
            'title': data_to_import['PAVADINIMAS'],
            'notes': data_to_import['SANTRAUKA'],
            'url': data_to_import['TINKLAPIS'],
            'tags': get_package_tags(data_to_import['R_ZODZIAI']),
            'maintainer': data_to_import['USER_NAME'],
            'maintainer_email': data_to_import['K_EMAIL'],
            'owner_org': local_org,
        }
        return self._create_or_update_package(package_dict, harvest_object)
