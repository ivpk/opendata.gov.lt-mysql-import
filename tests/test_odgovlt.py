# -*- coding: utf-8 -*-
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from ckanext.harvest.harvesters.ckanharvester import CKANHarvester
from ckanext.harvest.tests.factories import (HarvestSourceObj, HarvestJobObj,
                                             HarvestObjectObj)
import mock_ckan
import mock
from ckan import model
from ckan.tests.helpers import reset_db
import ckanext.harvest.model as harvest_model
import json
import os
import os.path
from ckan.tests.factories import Organization
from ckanext.harvest.tests.lib import run_harvest


def was_last_job_considered_error_free():
    last_job = model.Session.query(harvest_model.HarvestJob) \
                    .order_by(harvest_model.HarvestJob.created.desc()) \
                    .first()
    job = MagicMock()
    job.source = last_job.source
    job.id = ''
    return bool(CKANHarvester._last_error_free_job(job))


class TestODGovLt(object):
    @classmethod
    def setup(cls):
        reset_db()
        harvest_model.setup()

    @classmethod
    def setUpClass(cls):
        if os.path.isfile('opendatagov.db'):
            os.system('rm opendatagov.db')
        db_url = 'sqlite:///opendatagov.db'
        con = create_engine(db_url)
        con.execute('''
          CREATE TABLE t_rinkmena
          (ID INTEGER PRIMARY KEY ASC, PAVADINIMAS varchar(250) NOT NULL,\
SANTRAUKA text, TINKLAPIS text COLLATE BINARY,\
K_EMAIL text COLLATE BINARY)
          ''')
        con.execute('''
          INSERT INTO t_rinkmena VALUES(1, 'Šilumos tiekimo licencijas\
 turinčių įmonių sąrašas', '​Šilumos tiekimo\
 licencijas turinčių įmonių sąrašas',\
 'http://www.vkekk.lt/siluma/Puslapiai/licencijavimas/licenciju-turetojai.aspx',\
'jonaiste.jusionyte@regula.lt')
          ''')
        con.execute('''
          INSERT INTO t_rinkmena VALUES(2, '2014 m. vidutinio\
 metinio paros eismo intensyvumo duomenys', 'Pateikiama\
 informacija: kelio numeris, ruožo pradžia, ruožo\
 pabaiga, vidutinis metinis paros eismo \
intensyvumas, metai, automobilių tipai',\
'http://lakd.lrv.lt/lt/atviri-duomenys/vidutinio-m\
etinio-paros-eismo-intensyvumo-valstybines-reik\
smes-keliuose-duomenys-2013-m', 'vytautas.timukas@lakd.lt')
          ''')

    def test_gather(self):
        db_url = 'sqlite:///opendatagov.db'
        con = create_engine(db_url)
        meta = MetaData(bind=con, reflect=True)

        class Tables(object):
            rinkmena = meta.tables['t_rinkmena']

        clause = Tables.rinkmena.select()
        opendatagov_database = dict()
        for row in con.execute(clause):
            opendatagov_database[row[0]] = row
        with mock.patch('ckanext.harvest.harvesters.\
ckanharvester.config') as config:
            config.__getitem__.side_effect = db_url.split()
            source = HarvestSourceObj(
                      url='http://localhost:%s/' % mock_ckan.PORT)
            job = HarvestJobObj(source=source)
            harvester = CKANHarvester()
            obj_ids = harvester.gather_stage(job)
        assert job.gather_errors == []
        assert isinstance(obj_ids, list)
        assert len(obj_ids) == len(opendatagov_database)
        harvest_object = harvest_model.HarvestObject.get(obj_ids[0])
        assert harvest_object.guid == str(opendatagov_database.keys()[0])
        content = json.loads(harvest_object.content)
        assert content['ID'] == opendatagov_database[1][0]
        assert content['PAVADINIMAS'] == opendatagov_database[1][1]
        assert content['SANTRAUKA'] == opendatagov_database[1][2]
        harvest_object = harvest_model.HarvestObject.get(obj_ids[1])
        assert harvest_object.guid == str(opendatagov_database.keys()[1])
        content = json.loads(harvest_object.content)
        assert content['ID'] == opendatagov_database[2][0]
        assert content['PAVADINIMAS'] == opendatagov_database[2][1]
        assert content['SANTRAUKA'] == opendatagov_database[2][2]

    def test_fetch(self):
        source = HarvestSourceObj(url='http://localhost:%s/' % mock_ckan.PORT)
        job = HarvestJobObj(source=source)
        harvest_object = HarvestObjectObj(
            guid=mock_ckan.DATASETS[0]['id'],
            job=job,
            content=json.dumps(mock_ckan.DATASETS[0]))
        harvester = CKANHarvester()
        result = harvester.fetch_stage(harvest_object)
        assert harvest_object.errors == []
        assert result

    def test_import(self):
        db_url = 'sqlite:///opendatagov.db'
        con = create_engine(db_url)
        meta = MetaData(bind=con, reflect=True)

        class Tables(object):
            rinkmena = meta.tables['t_rinkmena']

        clause = Tables.rinkmena.select().where(Tables.rinkmena.c.ID == 1)
        opendatagov_database = dict()
        for row in con.execute(clause):
            for row_name in row.keys():
                opendatagov_database[row_name] = row[row_name]
        org = Organization()
        harvest_object = HarvestObjectObj(
            guid=opendatagov_database['ID'],
            content=json.dumps(opendatagov_database),
            job__source__owner_org=org['id'])
        harvester = CKANHarvester()
        result = harvester.import_stage(harvest_object)
        assert harvest_object.errors == []
        assert result
        assert harvest_object.package_id

    def test_harvest(self):
        db_url = 'sqlite:///opendatagov.db'
        con = create_engine(db_url)
        meta = MetaData(bind=con, reflect=True)

        class Tables(object):
            rinkmena = meta.tables['t_rinkmena']

        clause = Tables.rinkmena.select().where(Tables.rinkmena.c.ID == 1)
        opendatagov_database = dict()
        for row in con.execute(clause):
            for row_name in row.keys():
                opendatagov_database[row_name] = row[row_name]
        with mock.patch('ckanext.harvest.harvesters.\
ckanharvester.config') as config:
            config.__getitem__.side_effect = db_url.split()
            results_by_guid = run_harvest(
                url='http://localhost:%s/' % mock_ckan.PORT,
                harvester=CKANHarvester())
        result = results_by_guid['1']
        assert result['state'] == 'COMPLETE'
        assert result['report_status'] == 'added'
        assert result['errors'] == []
