# coding: utf-8

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division

import re
import string
import logging
import logging.config
import itertools

import unidecode
import sqlalchemy as sa

from ckan.plugins import toolkit
from ckan.logic import NotFound


logger = logging.getLogger(__name__)


def configure_logging(config):
    logging.config.dictConfig(config)
    for logger in map(logging.getLogger, config['loggers'].keys()):
        logger.disabled = 0


def dump_loggers():
    for name, logger in [('root', logging.root)] + logging.Logger.manager.loggerDict.items():
        if getattr(logger, 'handlers', None):
            print('%s: (level=%s, disabled=%r)' % (name, logging.getLevelName(logger.level), logger.disabled))
            for handler in logger.handlers:
                print('   %s.%s (level=%s)' % (
                    handler.__class__.__module__,
                    handler.__class__.__name__,
                    logging.getLevelName(handler.level),
                ))


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
        index = (i for i in itertools.chain.from_iterable(index) if i is not None)
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
    return ' '.join(spl)


def fixcase(value):
    if len(value) > 1 and value[:2].isalpha() and value[0].isupper() and value[1].islower():
        return value[0].lower() + value[1:]
    else:
        return value


def find_unique_name(names, name):
    unique = name
    counter = itertools.count()
    while unique in names:
        unique = '%s-%d' % (name, next(counter))
    names.add(unique)
    return unique


class CkanAPI(object):
    """Wrapper around CKAN API actions.

    See: http://docs.ckan.org/en/latest/api/index.html#action-api-reference
    """

    def __getattr__(self, name):
        return lambda context={}, **kwargs: toolkit.get_action(name)(context, kwargs)


SOURCE_ID_KEY = 'Išorinis ID'
CODE_KEY = 'Kodas'
ADDRESS_KEY = 'Adresas'


def get_package_tags(r_zodziai):
    if r_zodziai:
        tags = map(fixcase, map(string.strip, r_zodziai.replace(';', ',').split(',')))
        for tag in filter(None, tags):
            name = tagify(tag).lower()

            if len(name) > 100:
                logger.warn("skip very long tag: %r", tag)
            else:
                yield {
                    'name': name,
                }


class CkanSync(object):

    def __init__(self, ckanapi, db, conn):
        self.api = ckanapi
        self.db = db
        self.conn = conn
        self.execute = conn.execute

        class Tables(object):
            user = db.tables['t_user']
            istaiga = db.tables['t_istaiga']
            rinkmena = db.tables['t_rinkmena']
            rinkmenu_logas = db.tables['t_rinkmenu_logas']

        self.t = Tables

        self.importbot = self.sync_importbot_user()

    def sync_importbot_user(self):
        from ckan import model

        username = 'importbot'
        user = model.User.by_name(username)

        if not user:
            # TODO: How to deal with passwords?
            user = model.User(name=username, password='secret')
            user.sysadmin = True
            model.Session.add(user)
            model.repo.commit_and_remove()

        return self.api.user_show(id=username)

    def sync_user(self, user_id):
        user = self.execute(
            sa.select([self.t.user]).where(self.t.user.c.ID == user_id)
        ).fetchone()

        if user:
            user_data = {
                'name': slugify(user.LOGIN),
                # TODO: Passwrods are encoded with md5 hash, I need to look if
                #       CKAN supports md5 hashed passwords. If not, then users will
                #       have to change their passwords.
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
                'password': 'secret',
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

    def sync_organization(self, istaiga_id):
        organization = self.execute(
            sa.select([self.t.istaiga]).where(self.t.istaiga.c.ID == istaiga_id)
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
            context = {'user': self.importbot['name']}
            ckan_organization = self.api.organization_create(context, **organization_data)

        organization_data['id'] = ckan_organization['id']

        return organization_data

    def sync_datasets(self):
        taken_names = set()
        existing_datasets = {}
        logger.info('fetch existing datasets')
        for name in self.api.package_list():
            taken_names.add(name)
            ds = self.api.package_show(id=name)
            import_source_id = next((x['value'] for x in ds['extras'] if x['key'] == SOURCE_ID_KEY), None)
            if import_source_id:
                existing_datasets[int(import_source_id)] = ds['name']

        query = (
            sa.select([self.t.rinkmena]).
            where(self.t.rinkmena.c.STATUSAS == 'U').
            where(self.t.rinkmena.c.ID == 1051)
        )
        for row in self.execute(query):
            logger.info('sync package: %s', row.PAVADINIMAS)

            user = self.sync_user(row.USER_ID)
            organization = self.sync_organization(row.istaiga_id)
            self.api.organization_member_create(
                {'user': self.importbot['name']},
                id=organization['name'],
                username=user['name'],
                role='editor',
            )

            context = {
                'user': user['name'],
            }

            package = {
                # PAVADINIMAS
                'title': row.PAVADINIMAS,

                # SANTRAUKA
                'notes': row.SANTRAUKA,

                # TINKLAPIS
                'url': row.TINKLAPIS,

                # USER_ID
                'maintainer': user['fullname'],

                # K_EMAIL
                'maintainer_email': row.K_EMAIL,

                # istaiga_id
                'owner_org': organization['name'],

                # R_ZODZIAI
                'tags': list(get_package_tags(row.R_ZODZIAI)),

                'private': row.STATUSAS != 'U',
                'state': 'active',
                'type': 'dataset',

                'extras': [
                    # ID
                    {'key': SOURCE_ID_KEY, 'value': row.ID},

                    # KODAS
                    {'key': CODE_KEY, 'value': row.KODAS},
                ],
            }

            if row.ID not in existing_datasets:
                name = find_unique_name(taken_names, slugify(row.PAVADINIMAS))

                if name == '':
                    logger.warn('skip package with an empty name, package id: %s, code: %s', row.ID, row.KODAS)
                    continue

                created, modified = self.execute(
                    sa.select([
                        sa.func.min(self.t.rinkmenu_logas.c.DATA),
                        sa.func.max(self.t.rinkmenu_logas.c.DATA),
                    ]).
                    where(self.t.rinkmenu_logas.c.RINKMENOS_ID == row.ID)
                ).fetchone()

                package.update({
                    'name': name,
                    'metadata_created': created,
                    'metadata_modified': modified,
                })

                logger.debug('create new package: %s', name)
                self.api.package_create(context, **package)
            else:
                logger.debug('update existing package: %s (external id: %s)', existing_datasets[row.ID], row.ID)
                ds = self.api.package_show(id=existing_datasets[row.ID])
                ds.update(package)
                self.api.package_update(context, **ds)


class OpenDataGovLtCommand(toolkit.CkanCommand):
    """Synchronize old MySQL data with new CKAN database.

    Usage:

        paster --plugin=odgovlt-mysql-import odgovltsync -c ../deployment/ckan/development.ini -l debug

    """

    summary = __doc__.splitlines()[0]
    usage = __doc__

    def __init__(self, name):
        super(OpenDataGovLtCommand, self).__init__(name)

        self.parser.add_option('-l', '--level', dest='loglevel', default='info',
                               help='Sel log level')

    def command(self):
        self._load_config()

        log_level = self.options.loglevel.upper()
        configure_logging({
            'formatters': {
                'default': {
                    'format': '%(asctime)s %(levelname)7s %(name)s: %(message)s',
                    'datefmt': '%Y-%m-%d %H:%M:%S',
                },
            },
            'handlers': {
                'stderr': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'default',
                },
            },
            'loggers': {
                '': {'handlers': []},
                logger.name: {'level': log_level, 'handlers': ['stderr']},
                'ckan.logic.action.create': {'level': log_level, 'handlers': ['stderr']},
                'ckan.logic.action.delete': {'level': log_level, 'handlers': ['stderr']},
                'ckan.logic.action.patch':  {'level': log_level, 'handlers': ['stderr']},  # noqa
                'ckan.logic.action.update': {'level': log_level, 'handlers': ['stderr']},
            },
            'version': 1,
        })

        logger.info('connecting to opendata.gov.lt database')
        engine = sa.create_engine('mysql+pymysql://sirex:@localhost/rinkmenos?charset=utf8')
        db = sa.MetaData()
        db.reflect(bind=engine)
        conn = engine.connect()

        ckanapi = CkanAPI()
        sync = CkanSync(ckanapi, db, conn)
        logger.info('synchronisation started')
        sync.sync_datasets()
        logger.info('synchronisation ended')
