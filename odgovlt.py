# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import collections
import datetime
import itertools
import json
import logging
import re
import string

import sqlalchemy as sa
import unidecode

from ckan import model
from ckan.logic import NotFound
from ckan.plugins import toolkit
from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.model import HarvestObject
from pylons import config
from lxml import html
from cache.cache import Cache
from os.path import basename, splitext
import requests
import lxml
import rfc6266
import urlparse


log = logging.getLogger(__name__)
cache = Cache()

CODE_KEY = 'Kodas'
ADDRESS_KEY = 'Adresas'
SOURCE_ID_KEY = 'Šaltinio ID'
SOURCE_NAME = 'Šaltinis'
SOURCE_IVPK_IRS = 'IVPK IRS'


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


def get_web(base_url, time=20, headers={'User-Agent': 'Custom user agent'}):
    cache_dict = {}
    cache_list = []
    substring = [
        'pdf', 'doc', 'dot', 'xlsx', 'xls', 'xlt', 'xla', 'zip', 'csv', 'docx',
        'ppt', 'pot', 'pps', 'ppa', 'pptx', 'xlt', 'xla', 'xlw', 'ods']
    not_allowed_substring = [
        'mailto', 'aspx', 'javascript', 'duk', 'naudotojo_vadovas']
    try:
        page = requests.get(base_url, timeout=time, headers=headers)
        tree = html.fromstring(page.content)
        type = page.headers.get('content-type')
        op = type.startswith('text/html')
        page.close()
    except (
            requests.exceptions.InvalidSchema,
            requests.exceptions.MissingSchema,
            requests.exceptions.ConnectionError,
            lxml.etree.ParserError,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ReadTimeout,
            requests.exceptions.InvalidURL,
            AttributeError) as e:
        log.error(e)
        return
    if not op:
        return
    path = tree.xpath('//@href')
    for current in path:
        full_url = urlparse.urljoin(base_url, current)
        url_dict = {
            'website': base_url,
            'url': full_url}
        if not cache.__contains__(url_dict):
            cache_dict['website'] = base_url
            cache_dict['url'] = full_url
            try:
                disposition = requests.get(full_url,
                                           timeout=time,
                                           headers=headers,
                                           stream=True).headers.get('content-disposition')
                print disposition
                if disposition is None:
                    parse_url = requests.utils.urlparse(full_url)
                    filename = basename(parse_url.path)
                else:
                    try:
                        filename = rfc6266.parse_headers(disposition).filename_unsafe
                    except ValueError as e:
                        log.error(e)
                        try:
                            filename = re.findall("filename=(.+)", disposition)[0]
                        except IndexError as e:
                            log.error(e)
                type = splitext(filename)[1][1:].lower()
                if not type:
                    type = 'Unknown extension'
            except (
                    requests.exceptions.InvalidSchema,
                    requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.ReadTimeout,
                    requests.exceptions.InvalidURL,
                    NameError, UnboundLocalError, AttributeError) as e:
                log.error(e)
            try:
                cache_dict['name'] = filename
                cache_dict['type'] = type
                if type == 'Unknown extension':
                    cache_dict['is_data'] = False
                    cache_dict['cached_forever'] = False
                elif any(x in type for x in substring) and not any(x in full_url for x in not_allowed_substring):
                    cache_dict['is_data'] = True
                    cache_dict['cached_forever'] = False
                else:
                    cache_dict['is_data'] = False
                    cache_dict['cached_forever'] = True
            except (NameError, TypeError, AttributeError, UnboundLocalError) as e:
                log.error(e)
                cache_dict['name'] = 'Unknown name'
                cache_dict['type'] = 'Unknown type'
                cache_dict['is_data'] = False
                cache_dict['cached_forever'] = False
            cache_list.append(dict(cache_dict))
    return cache_list


def make_cache(url):
    new_caches = get_web(url)
    if new_caches is None:
        return
    for cache_dict in new_caches:
        try:
            cache.update(cache_dict)
        except KeyError as e:
            log.error(e)


class CkanAPI(object):
    """Wrapper around CKAN API actions.
    See: http://docs.ckan.org/en/latest/api/index.html#action-api-reference
    """

    def __init__(self, context=None):
        self.context = context or {}

    def __getattr__(self, name):
        def wrapper(context=None, **kwargs):
            context = dict(context) if context else dict(self.context)
            return toolkit.get_action(name)(context, kwargs)
        return wrapper


def was_changed(new, old, object_name, path=()):
    if isinstance(new, dict):
        for key in new:
            if was_changed(new[key], old.get(key), object_name, path + (key,)):
                return True
    elif isinstance(new, list):
        for i in range(len(new)):
            if i >= len(old) or was_changed(new[i], old[i], object_name, path + (i,)):
                return True
    elif new != old:
        log.debug('%s has been changed %r != %r', '.'.join(map(str, path)), new, old)
        return True
    return False


def extras_to_dict(extras):
    return {x['key']: x['value'] for x in extras}


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


class IvpkIrsSync(object):

    def __init__(self, engine):
        self.engine = engine
        meta = sa.MetaData(bind=engine)
        meta.reflect()
        tables = {
            'user': meta.tables['t_user'],
            'istaiga': meta.tables['t_istaiga'],
            'rinkmena': meta.tables['t_rinkmena'],
            'kategorija': meta.tables['t_kategorija'],
            'kategorija_rinkmena': meta.tables['t_kategorija_rinkmena'],
        }
        self.t = collections.namedtuple('Tables', tables.keys())(**tables)

        self.api = CkanAPI({'user': self.sync_harvest_user()})

    def sync_harvest_user(self):
        name = config.get('ckanext.harvest.user_name') or 'harvest'
        context = {'model': model, 'ignore_auth': True}

        try:
            user = toolkit.get_action('user_show')(context, {'id': name})
        except toolkit.ObjectNotFound:
            log.info('create %r sysadmin user', name)
            user = model.User(name=name, password='secret123')
            user.sysadmin = True
            model.Session.add(user)
            model.Session.commit()
        else:
            if not user['sysadmin']:
                log.info('add sysadmin priviledges to %r user', name)
                user = model.User.get(user['id'])
                user.sysadmin = True
                user.save()
                model.Session.add(user)
                model.Session.commit()

        return name

    def sync_user(self, user_id):
        ivpk_user = self.engine.execute(sa.select([self.t.user]).  where(self.t.user.c.ID == user_id)).fetchone()
        if ivpk_user:
            user_data = {
                'name': slugify(ivpk_user.LOGIN),
                # TODO: Passwrods are encoded with md5 hash, I need to look if
                #       CKAN supports md5 hashed passwords. If not,
                #       then users will have to change their passwords.
                'email': ivpk_user.EMAIL,
                'password': ivpk_user.PASS,
                'fullname': ' '.join([ivpk_user.FIRST_NAME, ivpk_user.LAST_NAME]),
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
            ckan_user = self.api.user_create(**user_data)

        user_data['id'] = ckan_user['id']

        return user_data

    def sync_organization(self, istaiga_id):
        organization = self.engine.execute(
            sa.select([self.t.istaiga]).
            where(self.t.istaiga.c.ID == istaiga_id)
        ).fetchone()

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
            ckan_organization = self.api.organization_show(id=organization_data['name'])
        except NotFound:
            ckan_organization = None

        if ckan_organization is None:
            ckan_organization = self.api.organization_create(**organization_data)

        organization_data['id'] = ckan_organization['id']
        return organization_data

    def sync_group_tree(self, ckan_group_names, ivpk_groups, ivpk_parent_group_id=0):
        for group_name, ivpk_group in ivpk_groups[ivpk_parent_group_id]:
            group_data = {
                'name': group_name,
                'title': ivpk_group.PAVADINIMAS,
                'extras': [
                    {'key': SOURCE_NAME, 'value': SOURCE_IVPK_IRS},
                    {'key': SOURCE_ID_KEY, 'value': ivpk_group.ID},
                ],
                'groups': [
                    {'name': ckan_group['name']}
                    for ckan_group in self.sync_group_tree(ckan_group_names, ivpk_groups, ivpk_group.ID)
                ],
                'state': 'active',
            }

            if group_name in ckan_group_names:
                ckan_group = self.api.group_show(id=group_name)
                if was_changed(group_data, ckan_group, 'group'):
                    group_data['id'] = group_name
                    log.info('update group: %s', group_name)
                    ckan_group = self.api.group_patch(**group_data)
                else:
                    log.debug('group is up to date: %s', group_name)
                yield ckan_group
            else:
                log.info('create group: %s', group_name)
                yield self.api.group_create(**group_data)

    def _get_group_name(self, ivpk_group):
        return slugify(ivpk_group.PAVADINIMAS + ' ' + str(ivpk_group.ID))

    def sync_groups(self):
        # We can't use api.group_list, becuase we also need list of deleted groups.
        ckan_group_names = set(
            group_name
            for group_name, in (
                model.Session.query(model.Group.name).
                filter(model.Group.is_organization == False)  # noqa
            )
        )
        ivpk_group_names = set()
        ivpk_groups = collections.defaultdict(list)

        for ivpk_group in self.engine.execute(sa.select([self.t.kategorija])):
            group_name = self._get_group_name(ivpk_group)
            ivpk_groups[ivpk_group.KATEGORIJA_ID].append((group_name, ivpk_group))
            ivpk_group_names.add(group_name)

        for _ in self.sync_group_tree(ckan_group_names, ivpk_groups):
            pass

        stale_ckan_groups = ckan_group_names - ivpk_group_names
        for group_name in stale_ckan_groups:
            ckan_group = self.api.group_show(id=group_name)
            ckan_group_extras = extras_to_dict(ckan_group['extras'])
            if ckan_group_extras.get(SOURCE_NAME) == SOURCE_IVPK_IRS:
                log.info('delete stale group: %s', ckan_group['name'])
                self.api.group_delete(id=ckan_group['id'])

    def get_package_groups(self, dataset_id):
        dataset_ivpk_group_ids = set([
            row.KATEGORIJA_ID
            for row in self.engine.execute(
                sa.select([self.t.kategorija_rinkmena]).
                where(self.t.kategorija_rinkmena.c.RINKMENA_ID == dataset_id)
            )
        ])

        for ivpk_group_id in dataset_ivpk_group_ids:
            ivpk_group = self.engine.execute(
                sa.select([self.t.kategorija]).
                where(self.t.kategorija.c.ID == ivpk_group_id)
            ).first()
            yield self._get_group_name(ivpk_group)

    def get_ivpk_datasets(self):
        query = (
            sa.select([self.t.rinkmena]).
            where(self.t.rinkmena.c.STATUSAS == 'U')
        )
        for ivpk_dataset in self.engine.execute(query):
            yield ivpk_dataset


class OdgovltHarvester(HarvesterBase):

    def info(self):
        return {
            'name': 'opendata-gov-lt',
            'title': 'opendata.gov.lt',
            'description': 'Harvest opendata.gov.lt',
            'form_config_interface': 'Text',
        }

    def gather_stage(self, harvest_object):
        log.debug('In OdgovltHarvester gather_stage')

        sync = IvpkIrsSync(sa.create_engine(harvest_object.source.url))
        sync.sync_groups()

        ids = []
        for ivpk_dataset in sync.get_ivpk_datasets():
            content = json.dumps(dict(ivpk_dataset), cls=DatetimeEncoder)
            obj = HarvestObject(guid=ivpk_dataset.ID, job=harvest_object, content=content)
            obj.save()
            ids.append(obj.id)
        return ids

    def fetch_stage(self, harvest_object):
        log.debug('In OdgovltHarvester fetch_stage')
        return True

    def import_stage(self, harvest_object):
        log.debug('In OdgovltHarvester import_stage')

        sync = IvpkIrsSync(sa.create_engine(harvest_object.source.url))

        ivpk_dataset = json.loads(harvest_object.content)
        user = sync.sync_user(ivpk_dataset['USER_ID'])
        organization = sync.sync_organization(ivpk_dataset['istaiga_id'])
        sync.api.organization_member_create(id=organization['name'], username=user['name'], role='editor')

        make_cache(ivpk_dataset['TINKLAPIS'])
        cache_list_to_import = []
        for cache_data in cache.get_url_data(ivpk_dataset['TINKLAPIS']):
            cache_list_to_import.append(
                {'url': cache_data['url'],
                 'name': cache_data['name'],
                 'format': cache_data['type']})
        package_dict = {
            'id': harvest_object.guid,
            'name': slugify(ivpk_dataset['PAVADINIMAS'], length=42),
            'title': ivpk_dataset['PAVADINIMAS'],
            'notes': ivpk_dataset['SANTRAUKA'],
            'url': ivpk_dataset['TINKLAPIS'],
            'maintainer': user['fullname'],
            'maintainer_email': ivpk_dataset['K_EMAIL'],
            'owner_org': organization['name'],
            'state': 'active',
            'resources': cache_list_to_import,
            'tags': [
                {'name': tag}
                for tag in get_package_tags(ivpk_dataset['R_ZODZIAI'])
            ],
            'groups': [
                {'name': ckan_group_name}
                for ckan_group_name in sync.get_package_groups(ivpk_dataset['ID'])
            ],
            'extras': [
                {'key': SOURCE_NAME, 'value': SOURCE_IVPK_IRS},
                {'key': SOURCE_ID_KEY, 'value': ivpk_dataset['ID']},
                {'key': CODE_KEY, 'value': ivpk_dataset['KODAS']},
            ],
        }
        return self._create_or_update_package(package_dict, harvest_object, package_dict_form='package_show')
