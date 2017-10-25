# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import collections
import contextlib
import gettext
import json
import logging
import os

from ckanext.harvest.harvesters.ckanharvester import CKANHarvester
from ckanext.harvest.tests.factories import (
                          HarvestSourceObj,
                          HarvestJobObj,
                          HarvestObjectObj)
from ckanext.harvest.tests.lib import run_harvest
from ckan.tests.helpers import reset_db
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

from odgovlt import OdgovltHarvester
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
    mocker.patch('ckan.lib.search.index.make_connection')

    db.engine.execute(db.meta.tables['t_rinkmena'].insert(), {
        'PAVADINIMAS': 'Šilumos tiekimo licencijas turinčių įmonių sąrašas',
        'SANTRAUKA': 'Šilumos tiekimo licencijas turinčių įmonių sąrašas',
        'TINKLAPIS': 'http://www.vkekk.lt/siluma/Puslapiai/'
                     'licencijavimas/licenciju-turetojai.aspx',
        'R_ZODZIAI': '​Šilumos tiekimo licencijas turinčių įmonių sąrašas,'
                     'šiluma,'
                     'šilumos tiekėjai,'
                     'licencijos,'
                     'licencijuojamos veiklos teritorija',
        'K_EMAIL': 'jonaiste.jusionyte@regula.lt',
    })

    db.engine.execute(db.meta.tables['t_rinkmena'].insert(), {
        'PAVADINIMAS': '2014 m. vidutinio metinio paros '
                       'eismo intensyvumo duomenys',
        'SANTRAUKA': 'Pateikiama informacija: kelio numeris, ruožo pradžia, '
                     'ruožo pabaiga, vidutinis metinis paros eismo '
                     'intensyvumas, metai, automobilių tipai',
        'TINKLAPIS': 'http://lakd.lrv.lt/lt/atviri-duomenys/'
                     'vidutinio-metinio-paros-eismo-intensyvumo-valstybines-'
                     'reiksmes-keliuose-duomenys-2013-m',
        'R_ZODZIAI': 'keliai,eismo intensyvumas',
        'K_EMAIL': 'vytautas.timukas@lakd.lt',
    })

    source = HarvestSourceObj(url='sqlite://', source_type='opendata-gov-lt')
    job = HarvestJobObj(source=source)
    harvester = OdgovltHarvester()
    obj_ids = harvester.gather_stage(job)
    assert job.gather_errors == []
    assert [json.loads(
               ckanext.harvest.model.HarvestObject.get(x).content
               )['PAVADINIMAS'] for x in obj_ids] == [
        'Šilumos tiekimo licencijas turinčių įmonių sąrašas',
        '2014 m. vidutinio metinio paros eismo intensyvumo duomenys',
    ]
    clause = db.meta.tables['t_rinkmena'].select()
    database_data_list = [dict(row) for row in db.engine.execute(clause)]
    obj1 = HarvestObjectObj(
        guid=database_data_list[0]['ID'],
        job=job,
        content=json.dumps(database_data_list[0], cls=DatetimeEncoder))
    result = harvester.fetch_stage(obj1)
    assert obj1.errors == []
    assert result
    obj2 = HarvestObjectObj(
        guid=database_data_list[1]['ID'],
        job=job,
        content=json.dumps(database_data_list[1], cls=DatetimeEncoder))
    result = harvester.fetch_stage(obj2)
    assert obj2.errors == []
    assert result
    create_or_update = harvester.import_stage(obj1)
    assert create_or_update
    create_or_update = harvester.import_stage(obj2)
    assert create_or_update
    assert obj1.package_id
    assert obj2.package_id
    reset_db()
    results_by_guid = run_harvest(
            url='sqlite://',
            harvester=OdgovltHarvester())
    result = results_by_guid['1']
    assert result['state'] == 'COMPLETE'
    assert result['report_status'] == 'added'
    assert result['errors'] == []
    result = results_by_guid['2']
    assert result['state'] == 'COMPLETE'
    assert result['report_status'] == 'added'
    assert result['errors'] == []
    assert was_last_job_considered_error_free()
    ckanapi = CkanAPI()
    ids = ckanapi.package_list()
    package = [ckanapi.package_show(id=ids[0])]
    assert [(x['title'],) for x in package] == [
        ('2014 m. vidutinio metinio paros '
         'eismo intensyvumo duomenys',),
    ]
    assert [(x['notes'],) for x in package] == [
        ('Pateikiama informacija: kelio numeris, ruožo pradžia, '
         'ruožo pabaiga, vidutinis metinis paros eismo '
         'intensyvumas, metai, automobilių tipai',),
    ]
    assert [(x['url'],) for x in package] == [
        ('http://lakd.lrv.lt/lt/atviri-duomenys/'
         'vidutinio-metinio-paros-eismo-intensyvumo-valstybines-'
         'reiksmes-keliuose-duomenys-2013-m',),
    ]
    assert [(x['maintainer_email'],) for x in package] == [
        ('vytautas.timukas@lakd.lt',),
    ]
    package = [ckanapi.package_show(id=ids[1])]
    assert [(x['title'],) for x in package] == [
        ('Šilumos tiekimo licencijas turinčių įmonių sąrašas',),
    ]
    assert [(x['notes'],) for x in package] == [
        ('Šilumos tiekimo licencijas turinčių įmonių sąrašas',),
    ]
    assert [(x['url'],) for x in package] == [
        ('http://www.vkekk.lt/siluma/Puslapiai/'
         'licencijavimas/licenciju-turetojai.aspx',),
    ]
    assert [(x['maintainer_email'],) for x in package] == [
        ('jonaiste.jusionyte@regula.lt',),
    ]
    tags = ckanapi.package_show(id=ids[0])['tags']
    assert sorted([x['name'] for x in tags]) == [
                       'eismo intensyvumas',
                       'keliai']
    tags = ckanapi.package_show(id=ids[1])['tags']
    assert sorted([x['name'] for x in tags]) == [
                       'licencijos',
                       'licencijuojamos veiklos teritorija',
                       'šiluma',
                       'šilumos tiekimo licencijas turinčių įmonių sąrašas',
                       'šilumos tiekėjai']
