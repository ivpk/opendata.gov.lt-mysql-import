from __future__ import print_function
from __future__ import unicode_literals

import sqlalchemy as sa

from ckan.plugins import toolkit


class CkanAPI(object):
    """Wrapper around CKAN API actions.

    See: http://docs.ckan.org/en/latest/api/index.html#action-api-reference
    """

    def __init__(self, **kwargs):
        self.context = kwargs

    def __getattr__(self, name):
        return lambda **kwargs: toolkit.get_action(name)(self.context, kwargs)


class OpenDataGovLtCommand(toolkit.CkanCommand):
    """Synchronize old MySQL data with new CKAN database.

    Usage:

        paster --plugin=ckan odgovltsync -c ckan/development.ini

    """

    summary = "Synchronize old MySQL data with new CKAN database."
    usage = __doc__

    def command(self):
        self._load_config()
        print(self.args)

        ckan = CkanAPI()
        print(ckan.status_show())

        engine = sa.create_engine('mysql+pymysql://sirex:@localhost/rinkmenos?charset=utf8')
        db = sa.MetaData()
        db.reflect(bind=engine)
        conn = engine.connect()

        query = """
            SELECT
                t_rinkmena.*,
                t_istaiga.PAVADINIMAS AS istaiga
            FROM t_rinkmena
            LEFT JOIN t_istaiga ON (t_istaiga.ID = t_rinkmena.ISTAIGA_ID)
            LIMIT 10
        """

        for row in conn.execute(query):
            print(row['istaiga'], '::', row['PAVADINIMAS'])
