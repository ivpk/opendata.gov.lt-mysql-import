# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy import MetaData
from ckanext.harvest.model import HarvestObject
import logging
from base import HarvesterBase
from ckan.common import config

log = logging.getLogger(__name__)


class ODGovLt(HarvesterBase):
    def _connect_to_database(self):
        con = create_engine(config['odgovltimport.externaldb.url'])
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
        con, meta = self._connect_to_database()

        class Tables(object):
            rinkmena = meta.tables['t_rinkmena']

        clause = Tables.rinkmena.select()
        ids = []
        for row in con.execute(clause):
            re = Tables.rinkmena.c
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
