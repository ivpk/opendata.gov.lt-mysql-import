# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import collections
import contextlib
import gettext
import json
import logging
import os

from ckanext.harvest.harvesters.ckanharvester import CKANHarvester
from ckanext.harvest.harvesters.base import HarvesterBase
from ckan.tests.factories import Organization
from ckanext.harvest.tests.factories import (
                          HarvestSourceObj,
                          HarvestJobObj,
                          HarvestObjectObj)
import ckan.lib.search.index
import ckan.config.middleware
import ckan.model
import ckanext.harvest.model
import mock
import pkg_resources as pres
import psycopg2
import psycopg2.extensions
import pylons
import pytest
import sqlalchemy as sa
import webtest

from odgovlt import DatetimeEncoder
from odgovlt import CkanAPI


class CKANTestApp(webtest.TestApp):
    '''A wrapper around webtest.TestApp
    It adds some convenience methods for CKAN
    '''

    _flask_app = None

    @property
    def flask_app(self):
        if not self._flask_app:
            self._flask_app = self.app.apps['flask_app']._wsgi_app
        return self._flask_app


def was_last_job_considered_error_free():
    last_job = (
        ckan.model.Session.
        query(ckanext.harvest.model.HarvestJob).
        order_by(ckanext.harvest.model.HarvestJob.created.desc()).
        first()
    )
    job = mock.MagicMock()
    job.source = last_job.source
    job.id = ''
    return bool(CKANHarvester._last_error_free_job(job))


def sync_importbot_user():
    from ckan import model

    api = CkanAPI()
    username = 'importbot'
    user = model.User.by_name(username)

    if not user:
        # TODO: How to deal with passwords?
        user = model.User(name=username, password='secret123')
        user.sysadmin = True
        model.Session.add(user)
        model.repo.commit_and_remove()

    return api.user_show(id=username)


@pytest.fixture
def db():
    engine = sa.create_engine('sqlite://')
    meta = sa.MetaData()

    with open(os.path.join(os.path.dirname(__file__), 'schema.sql')) as f:
        engine.raw_connection().executescript(f.read())

    meta.reflect(bind=engine)

    return collections.namedtuple('DB', ('engine', 'meta'))(engine, meta)


@pytest.fixture
def postgres():
    dbname = 'odgovlt_mysql_import_tests'
    with contextlib.closing(psycopg2.connect(
                         'postgresql:///postgres')) as conn:
        conn.set_isolation_level(
                         psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        with contextlib.closing(conn.cursor()) as curs:
            curs.execute('DROP DATABASE IF EXISTS ' + dbname)
            curs.execute('CREATE DATABASE ' + dbname)
    return 'postgresql:///%s' % dbname


@pytest.fixture
def app(postgres, mocker, caplog):
    caplog.set_level(logging.WARNING, logger='ckan.lib.i18n')
    caplog.set_level(logging.WARNING, logger='migrate')
    caplog.set_level(logging.WARNING, logger='pyutilib')
    caplog.set_level(logging.WARNING, logger='vdm')
    caplog.set_level(logging.WARNING, logger='pysolr')

    mocker.patch('ckan.lib.search.check_solr_schema_version')

    global_config = {
        '__file__': '',
        'here': os.path.dirname(__file__),
        'ckan.site_url': 'http://localhost',
        'sqlalchemy.url': postgres,
        'ckan.plugins': 'harvest odgovlt_harvester',
    }
    app_config = {
        'who.config_file': pres.resource_filename('ckan.config', 'who.ini'),
        'beaker.session.secret': 'secret',
    }
    app = ckan.config.middleware.make_app(global_config, **app_config)
    app = CKANTestApp(app)

    ckan.model.repo.init_db()
    ckanext.harvest.model.setup()

    pylons.translator = gettext.NullTranslations()

    return app


def test_OdgovltHarvester(app, db, mocker):
    mocker.patch(
            'odgovlt.OdgovltHarvester._connect_to_database',
            return_value=db)

    db.engine.execute(db.meta.tables['t_rinkmena'].insert(), {
        'PAVADINIMAS': 'Testinė rinkmena nr. 1',
        'SANTRAUKA': 'Testas nr. 1',
        'TINKLAPIS': 'http://www.testas1.lt',
        'R_ZODZIAI': '​Šilumos tiekimo licencijas turinčių įmonių sąrašas,'
                     'šiluma,'
                     'šilumos tiekėjai,'
                     'licencijos,'
                     'licencijuojamos veiklos teritorija',
        'K_EMAIL': 'testas1@testas1.com',
        'STATUSAS': 'U',
        'USER_ID': 1,
        'istaiga_id': 1,
    })

    db.engine.execute(db.meta.tables['t_rinkmena'].insert(), {
        'PAVADINIMAS': 'Testinė rinkmena nr. 2',
        'SANTRAUKA': 'Testas nr. 2',
        'TINKLAPIS': 'http://www.testas2.lt',
        'R_ZODZIAI': 'keliai,eismo intensyvumas',
        'K_EMAIL': 'testas2@testas2.com',
        'STATUSAS': 'U',
        'USER_ID': 2,
        'istaiga_id': 2,
    })

    source = HarvestSourceObj(url='sqlite://', source_type='opendata-gov-lt')
    job = HarvestJobObj(source=source)
    org = Organization()
    clause = db.meta.tables['t_rinkmena'].select()
    database_data_list = [dict(row) for row in db.engine.execute(clause)]
    harvest_object = HarvestObjectObj(
        guid=database_data_list[0]['ID'],
        job=job,
        content=json.dumps(database_data_list[0], cls=DatetimeEncoder))
    api = CkanAPI()
    harvester_base = HarvesterBase()
    sync_importbot_user()
    context = {'user': 'importbot'}
    group_data = {
       'name': 'grupe'
    }
    api.group_create(context, **group_data)
    group = None
    group_list = api.group_list()
    assert 'grupe' in group_list
    try:
        group = api.group_show(context, id='grupe')
    except:
        pass
    assert group is not None
    data_to_import = json.loads(harvest_object.content)
    pavadinimas = data_to_import['PAVADINIMAS']
    package_dict = {
            'id': harvest_object.guid,
            'title': pavadinimas,
            'groups': [{'name': 'grupe'}],
            'owner_org': org['id'],
    }
    harvester_base._create_or_update_package(package_dict, harvest_object)
    groups1 = api.package_show(context, id='testin-rinkmena-nr-1')['groups']
    groups2 = api.member_list(id='grupe', object_type='package')
    assert groups1
    assert groups2
