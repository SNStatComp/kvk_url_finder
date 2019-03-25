from pathlib import Path

import peewee as pw
from playhouse.pool import (PooledPostgresqlExtDatabase)

KVK_KEY = "kvk_nummer"
BTW_KEY = "btw_nummer"
NAME_KEY = "naam"
URL_KEY = "url"
URLNL_KEY = "url_nl"
ADDRESS_KEY = "straat"
POSTAL_CODE_KEY = "postcode"
CITY_KEY = "plaats"
COMPANY_KEY = "company"
BEST_MATCH_KEY = "best_match"
LEVENSHTEIN_KEY = "levenshtein"
STRING_MATCH_KEY = "string_match"
RANKING_KEY = "ranking"
MAX_PROCESSES = 128

GETEST_KEY = "getest"
BESTAAT_KEY = "bestaat"
EXISTS_KEY = "exists"
DISTANCE_KEY = "distance"
HAS_POSTCODE_KEY = "has_postcode"
HAS_KVK_NR = "has_kvk_nr"
ECOMMERCE = "ecommerce"
SUBDOMAIN_KEY = "subdomain"
DOMAIN_KEY = "domain"
SUFFIX_KEY = "suffix"
CATEGORY_KEY = "category"

WEB_DF_COLS = [URL_KEY,
               EXISTS_KEY,
               DISTANCE_KEY,
               STRING_MATCH_KEY,
               HAS_POSTCODE_KEY,
               HAS_KVK_NR,
               SUBDOMAIN_KEY,
               DOMAIN_KEY,
               SUFFIX_KEY,
               RANKING_KEY
               ]

PRAGMAS = {
    "journal_mode": "wal",
    "foreingn_keys": 1,
    "ignore_check_constraints": 0,
    "synchronous": 0
}
DATABASE_TYPES = ("sqlite", "postgres")


def init_database(database_name: Path,
                  database_type="postgres", user="postgres",
                  password=None, host="localhost", port=5432):
    assert database_type in DATABASE_TYPES
    if database_type == "postgres":
        db = PooledPostgresqlExtDatabase(
            database_name, user=user, host=host, port=port, password=password,
            max_connections=MAX_PROCESSES, stale_timeout=300)
    elif database_type == "sqlite":
        db = pw.SqliteDatabase(str(database_name), pragmas=PRAGMAS)
    else:
        raise ValueError("Allowed database types:  {}".format(DATABASE_TYPES))
    return db


class UnknownField(object):
    def __init__(self, *_, **__): pass


def init_models(db, reset_tables=False):
    class BaseModel(pw.Model):
        class Meta:
            database = db
            only_save_dirty = True

    # this class describes the format of the sql data base
    class Company(BaseModel):
        kvk_nummer = pw.IntegerField(primary_key=True)
        naam = pw.CharField(null=True)
        ranking = pw.IntegerField(default=-1)
        core_id = pw.IntegerField(default=-1)  # also give the process number. If -1, not done
        datetime = pw.DateTimeField(null=True)  # the process time

    class UrlNL(BaseModel):
        """
        Tabel met unieke url's. Een URL kan nul of hooguit 1 kvk nummer hebben, omdat hooguit 1 bedrijf eigenaar
        van een url kan zijn. Dit is het verschil met de WebSite tabel, waarbij iedere url meerdere kvk's kan hebben
        omdat dat alleen de kvk zijn die op de site voorkomen, maar niet perse de eigenaars van de site
        """
        url = pw.CharField(null=True)
        getest = pw.BooleanField(default=False)
        bestaat = pw.BooleanField(default=False)
        kvk_nummer = pw.IntegerField(default=-1)
        btw_nummer = pw.IntegerField(default=-1)
        datetime = pw.DateTimeField(null=True)  # the process time
        subdomain = pw.CharField(null=True)
        domain = pw.CharField(null=True)
        suffix = pw.CharField(null=True)
        category = pw.IntegerField(default=-1)
        ecommerce = pw.IntegerField(default=-1)

    class Address(BaseModel):
        company = pw.ForeignKeyField(Company, backref="address")
        kvk_nummer = pw.IntegerField(default=-1)
        naam = pw.CharField(null=True)
        plaats = pw.CharField(null=True)
        postcode = pw.CharField(null=True)
        straat = pw.CharField(null=True)

    class WebSite(BaseModel):
        company = pw.ForeignKeyField(Company, backref="websites")
        kvk_nummer = pw.IntegerField(default=-1)
        url = pw.CharField(null=False)
        naam = pw.CharField(null=False)
        getest = pw.BooleanField(default=False)
        levenshtein = pw.IntegerField(default=-1)
        string_match = pw.FloatField(default=-1)
        best_match = pw.BooleanField(default=True)
        has_postcode = pw.BooleanField(default=False)
        has_kvk_nr = pw.BooleanField(default=False)
        ranking = pw.IntegerField(default=-1)
        bestaat = pw.BooleanField(default=False)

    tables = (UrlNL, Company, Address, WebSite)

    if db.is_closed():
        db.connect()
    if reset_tables and Company.table_exists():
        db.drop_tables(tables)
    db.create_tables(tables)

    return tables
