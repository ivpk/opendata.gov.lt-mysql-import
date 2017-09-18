# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy import MetaData
from ckanext.harvest.model import HarvestObject
import logging
from base import HarvesterBase

log = logging.getLogger(__name__)


class CKANHarvester(HarvesterBase):
    def _connect(self, user, password, db, host='192.168.56.101'):
        url = 'mysql://{}:{}@{}/{}?charset=utf8'
        url = url.format(user, password, host, db)
        con = create_engine(url)
        meta = MetaData(bind=con, reflect=True)
        return con, meta

    def info(self):
        return {
            'name': 'opendatagov',
            'title': 'opendata.gov.lt',
            'description': 'Harvest opendata.gov.lt',
            'form_config_interface': 'Text'
        }

    def gather_stage(self, harvest_job):
        log.debug('In opendatagov gather_stage')
        con, meta = self._connect('root', 'root', 'opendatagov')
        results = meta.tables['t_rinkmena']
        clause = results.select()
        ids = []
        for row in con.execute(clause):
            re = results.c
            id = row[re.ID]
            obj = HarvestObject(
                  guid=id,
                  job=harvest_job,
                  content='%s,%s,%s' %
                  (
                      row[re.ID],
                      row[re.PAVADINIMAS],
                      row[re.SANTRAUKA]
                  ))
            obj.save()
            ids.append(obj.id)
        return ids

    def fetch_stage(self, harvest_object):
        return True

    def import_stage(self, harvest_object):
        data_to_import = harvest_object.content.split(',')
        package_dict = {
                'id': data_to_import[0],
                'title': data_to_import[1],
                'notes': data_to_import[2],
                'owner_org': 'orga'
        }
        return self._create_or_update_package(package_dict, harvest_object)
