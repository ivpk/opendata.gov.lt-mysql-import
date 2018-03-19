from setuptools import setup, find_packages


setup(
    name='odgovlt-mysql-import',
    version='0.1',
    license='AGPL',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'Unidecode',
        'sqlalchemy',
        'PyMySQL',
        'rfc6266',
    ],
    extras_require={
        'tests': [
            'pytest',
            'pytest-catchlog',
        ]
    },
    entry_points={
        'ckan.plugins': [
            'odgovlt_harvester=odgovlt:OdgovltHarvester',
        ]
    },
)
