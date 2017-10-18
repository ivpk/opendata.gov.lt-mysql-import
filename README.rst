Harvester for opendata.gov.lt old database. This harvester connects directly to
MySQL database of old opendata.gov.lt website and harvestes datasets from there
to CKAN.

To install CKAN plugin you need to add it to CKAN configuration::

    ckan.plugins =
        harvest
        odgovlt_harvester


Connection parameters to MySQL database should be specified as harvester URL,
and it should looks like this::

    mysql+pymysql://usernaem:password@hostname/dbname?charset=utf8
