# -*- coding: utf-8 -*-

from __future__ import unicode_literals, print_function

from os.path import basename
import collections
import datetime
import itertools
import json
import logging
import re
import string
import cgi
import mimetypes
import robotparser
import urllib

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
import requests
import lxml
import urlparse

requests.packages.urllib3.disable_warnings()


log = logging.getLogger(__name__)

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


def get_top_level_domain(netloc):
    if '.' in netloc:
        split = netloc.lower().split('.')
        return '.'.join(split[-2:])
    else:
        return ''


def getext(value):
    parts = value.lower().split('.')
    if len(parts) <= 1 or parts[0] == '':
        return ''

    result = []
    for part in parts[1:][-2:][::-1]:
        if part and re.match(r'^[a-z0-9]{1,5}$', part) and not part.isdigit():
            result.append(part)
        else:
            break
    return '.'.join(result[::-1])


def check_url(base_url, full_url, robots=None, cache=None):
    # Skip unknown protocols
    purl = requests.utils.urlparse(full_url)
    if purl.scheme not in ('http', 'https', ''):
        return False

    # Skip all external urls
    base_purl = requests.utils.urlparse(base_url)
    base_domain = get_top_level_domain(base_purl.netloc)
    if base_domain != getext(purl.netloc):
        return False

    # Skip all urls restricted by robots.txt
    if robots and not robots.can_fetch('odgovlt', full_url.encode('utf-8')):
        return False

    # Skip cached urls
    if cache:
        url_dict = {'website': base_url, 'url': full_url}
        if url_dict in cache:
            return False

    return True


KNOWN_FILE_TYPES = [
    'pdf', 'doc', 'dot', 'xlsx', 'xls', 'xlt', 'xla', 'zip', 'csv', 'docx', 'ppt', 'pot', 'pps', 'ppa', 'pptx', 'xlt',
    'xla', 'xlw', 'ods'
]

IGNORE_FILE_TYPES = [
    'mailto', 'aspx', 'javascript', 'duk', 'naudotojo_vadovas'
]


def guess_resource(resp, base_url, full_url):
    filename = ''
    filetype = ''
    purl = requests.utils.urlparse(full_url)
    headers = dict(resp.headers)

    # Guess filename from content-disposition header
    disposition = headers.get('content-disposition')
    if disposition:
        value, params = cgi.parse_header(disposition)
        filename = params.get('filename', '')

    # Guess filename from content-type
    content_type = headers.get('content-type')
    if not filename and content_type:
        value, params = cgi.parse_header(content_type)
        filename = mimetypes.guess_extension(value)
        if filename:
            filename = urllib.unquote(basename(purl.path) + filename)

    # Guess filename from url path
    if not filename:
        filename = urllib.unquote(basename(purl.path))

    # Guess file format from guess filename
    if filename:
        filetype = getext(filename)

    if filetype == '':
        is_data = False
        cached_forever = False

    elif any(x in filetype for x in KNOWN_FILE_TYPES) and not any(x in full_url for x in IGNORE_FILE_TYPES):
        is_data = True
        cached_forever = False

    else:
        is_data = False
        cached_forever = True

    return {
        'website': base_url,
        'url': full_url,
        'name': filename,
        'type': filetype,
        'is_data': is_data,
        'cached_forever': cached_forever,
    }


def progressbar(items):
    for item in items:
        yield item


def guess_resource_urls(cache, base_url, timeout=20, headers=None, progressbar=progressbar):
    headers = headers or {'User-Agent': 'odgovlt'}
    session = requests.Session()
    session.headers.update(headers)
    session.timeout = timeout
    session.verify = False

    base_purl = requests.utils.urlparse(base_url)
    robots = robotparser.RobotFileParser(base_purl.scheme + '://' + base_purl.netloc + '/robots.txt')
    try:
        robots.read()
    except IOError:
        pass

    if not check_url(base_url, base_url, robots):
        return

    try:
        resp = session.get(base_url)
        tree = html.fromstring(resp.content)
    except (requests.exceptions.InvalidSchema,
            requests.exceptions.MissingSchema,
            requests.exceptions.ConnectionError,
            lxml.etree.ParserError,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ReadTimeout,
            requests.exceptions.InvalidURL,
            AttributeError,
            IOError) as e:
        log.warning("error while fetching and parsing base url: %s", e)
        return

    content_type = resp.headers.get('content-type')
    if content_type and not content_type.startswith(('text/html', 'text/xhtml')):
        # If base url itself is not an html page, then it might be a resource.
        yield guess_resource(resp, base_url, base_url)
        return

    # Visit all links found in base url and look for possible resource links.
    for href in progressbar(tree.xpath('//@href')):
        full_url = urlparse.urljoin(base_url, href)

        if not check_url(base_url, full_url, robots, cache):
            continue

        resp = None
        try:
            resp = session.get(full_url, stream=True)

        except (requests.exceptions.InvalidSchema,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.InvalidURL,
                NameError, UnboundLocalError, AttributeError, IOError) as e:
            log.warning("error while fetching and parsing page url: %s", e)

        else:
            yield guess_resource(resp, base_url, full_url)

        finally:
            if resp:
                resp.close()


def get_web(cache, base_url, timeout=20, headers=None, progressbar=progressbar):
    return [
        data for data in guess_resource_urls(cache, base_url, timeout, headers, progressbar)
    ]


def make_cache(cache, url):
    for cache_dict in get_web(cache, url):
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
        query = sa.select([self.t.rinkmena])
        for ivpk_dataset in self.engine.execute(query):
            yield ivpk_dataset


class OdgovltHarvester(HarvesterBase):

    def __init__(self, *args, **kwargs):
        super(HarvesterBase, self).__init__(*args, **kwargs)
        cache_dir = 'sqlite:///%s/cache.db' % config.get('cache_dir', '/tmp')
        log.info('odgovlt cache dir: %s', cache_dir)
        self._cache = Cache(cache_dir)

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

        make_cache(self._cache, ivpk_dataset['TINKLAPIS'])
        cache_list_to_import = []
        for cache_data in self._cache.get_url_data(ivpk_dataset['TINKLAPIS']):
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
            'state': 'active' if ivpk_dataset['STATUSAS'] == 'U' and ivpk_dataset['GALIOJA'] == 'T' else 'deleted',
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
