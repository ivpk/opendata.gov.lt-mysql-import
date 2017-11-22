# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import datetime
import itertools
import json
import logging
import re
import string
import ast

import sqlalchemy as sa
import unidecode

from ckan.logic import NotFound
from ckan.plugins import toolkit
from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.model import HarvestObject

log = logging.getLogger(__name__)

SOURCE_ID_KEY = 'Išorinis ID'
CODE_KEY = 'Kodas'
ADDRESS_KEY = 'Adresas'


def fixcase(value):
    if len(value) > 1 and value[:2].isalpha() and value[0].isupper() and value[1].islower():
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
                log.warning("skip very long tag: %r", tag)
            elif len(name) < 2:
                log.warning("skip too short tag: %r", tag)
            else:
                name_list.append(name)
    return name_list


class CkanAPI(object):
    """Wrapper around CKAN API actions.
    See: http://docs.ckan.org/en/latest/api/index.html#action-api-reference
    """

    def __getattr__(self, name):
        def wrapper(context=None, **kwargs):
            context = dict(context) if context else {}
            return toolkit.get_action(name)(context, kwargs)
        return wrapper


class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            try:
                return obj.strftime('%Y-%m-%dT%H:%M:%S')
            except ValueError:
                # strftime gives ValueError for 0000-00-00 00:00:00 datetimes.
                return None
        else:
            return super(DatetimeEncoder, obj).default(obj)


class OdgovltHarvester(HarvesterBase):

    def _connect_to_database(self, dburl):
        con = sa.create_engine(dburl)
        meta = sa.MetaData(bind=con, reflect=True)
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

    def sync_organization(self, istaiga_id, conn):
        organization = conn.execute(sa.select([self.t.istaiga]).where(self.t.istaiga.c.ID == istaiga_id)).fetchone()

        if organization:
            organization_data = {
                # PAVADINIMAS
                'name': slugify(organization.PAVADINIMAS),
                'title': organization.PAVADINIMAS,

                'state': 'active',

                'extras': [
                    # ID
                    {'key': SOURCE_ID_KEY, 'value': organization.ID},

                    # KODAS
                    {'key': CODE_KEY, 'value': organization.KODAS},

                    # ADRESAS
                    {'key': ADDRESS_KEY, 'value': organization.ADRESAS},
                ],
            }
        else:
            organization_data = {
                'name': 'unknown',
                'title': 'Unknown organization',
                'state': 'active',
            }

        try:
            ckan_organization = \
                self.api.organization_show(id=organization_data['name'])
        except NotFound:
            ckan_organization = None

        if ckan_organization is None:
            context = {'user': self.importbot['name']}
            ckan_organization = self.api.organization_create(context, **organization_data)

        organization_data['id'] = ckan_organization['id']
        return organization_data

    def sync_group(self, grupe_id, conn):
        self.api = CkanAPI()
        self.importbot = self.sync_importbot_user()

        group = conn.execute(sa.select([self.t.kategorija]).where(self.t.kategorija.c.ID == grupe_id)).fetchone()

        group_data = {
            'id': group.ID,
            'name': slugify(group.PAVADINIMAS),
        }

        extra_data = {
            'ID': str(group.ID),
            'KATEGORIJA_ID': str(group.KATEGORIJA_ID),
            'LYGIS': group.LYGIS
        }

        try:
            ckan_group = \
                self.api.group_show(id=group_data['name'])
        except NotFound:
            ckan_group = None

        if ckan_group is None:
            context = {'user': self.importbot['name']}
            ckan_group = \
                self.api.group_create(context, **group_data)
        return extra_data

    def info(self):
        return {
            'name': 'opendata-gov-lt',
            'title': 'opendata.gov.lt',
            'description': 'Harvest opendata.gov.lt',
            'form_config_interface': 'Text'
        }

    def gather_stage(self, harvest_job):
        log.debug('In OdgovltHarvester gather_stage')
        con, meta = self._connect_to_database(harvest_job.source.url)

        class Tables(object):
            user = meta.tables['t_user']
            istaiga = meta.tables['t_istaiga']
            rinkmena = meta.tables['t_rinkmena']
            kategorija = meta.tables['t_kategorija']
            kategorija_rinkmena = meta.tables['t_kategorija_rinkmena']

        self.t = Tables

        ids = []
        database_data = {}
        conn = con.connect()
        query = (
            sa.select([self.t.rinkmena]).
            where(self.t.rinkmena.c.STATUSAS == 'U')
        )
        query2 = (sa.select([self.t.kategorija]))
        group_data = []
        for row in con.execute(query2):
            group = self.sync_group(row.ID, conn)
            group_data.append(group)
        context = {'user': self.importbot['name']}
        for row in group_data:
            data_dict = {}
            id = row['ID']
            groups = []
            for row2 in group_data:
                if id == row2['KATEGORIJA_ID']:
                    groups.append({'name': row2['ID']})
            data_dict['id'] = id
            data_dict['groups'] = groups
            self.api.group_patch(context, **data_dict)
        query3 = (sa.select([self.t.kategorija_rinkmena]))
        kategorija_rinkmena_data = []
        for row in con.execute(query3):
            kategorija_rinkmena_data.append({
                    'ID': row.ID,
                    'KATEGORIJA_ID': row.KATEGORIJA_ID,
                    'RINKMENA_ID': row.RINKMENA_ID})
        for row in con.execute(query):
            database_data = dict(row)
            id = database_data['ID']
            user = self.sync_user(row.USER_ID, conn)
            organization = self.sync_organization(row.istaiga_id, conn)
            database_data['VARDAS'] = user['fullname']
            database_data['ORGANIZACIJA'] = organization['name']
            database_data['KATEGORIJA_RINKMENA'] = str(kategorija_rinkmena_data)
            self.api.organization_member_create(
                {'user': self.importbot['name']},
                id=organization['name'],
                username=user['name'],
                role='editor',
            )
            obj = HarvestObject(guid=id, job=harvest_job, content=json.dumps(database_data, cls=DatetimeEncoder))
            obj.save()
            ids.append(obj.id)
        return ids

    def fetch_stage(self, harvest_object):
        log.debug('In OdgovltHarvester fetch_stage')
        return True

    def import_stage(self, harvest_object):
        log.debug('In OdgovltHarvester import_stage')
        data_to_import = json.loads(harvest_object.content)
        pavadinimas = data_to_import['PAVADINIMAS']
        package_dict = {
            'id': harvest_object.guid,
            'title': pavadinimas,
            'notes': data_to_import['SANTRAUKA'],
            'url': data_to_import['TINKLAPIS'],
            'name': slugify(pavadinimas, length=42),
            'tags': get_package_tags(data_to_import['R_ZODZIAI']),
            'maintainer': data_to_import['VARDAS'],
            'maintainer_email': data_to_import['K_EMAIL'],
            'owner_org': data_to_import['ORGANIZACIJA'],
        }
        self._create_or_update_package(package_dict, harvest_object)
        kategorija_rinkmena = ast.literal_eval(data_to_import['KATEGORIJA_RINKMENA'])
        context = {'user': self.importbot['name']}
        data_dict = {}
        data_dict['id'] = package_dict['id']
        groups = []
        for data in kategorija_rinkmena:
            if str(data['RINKMENA_ID']) == package_dict['id']:
                groups.append({'id': data['KATEGORIJA_ID']})
        data_dict['groups'] = groups
        self.api.package_patch(context, **data_dict)
        return True
