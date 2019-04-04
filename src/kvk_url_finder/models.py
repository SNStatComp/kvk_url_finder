from pathlib import Path
import logging

import peewee as pw
from playhouse.pool import (PooledPostgresqlExtDatabase)
from playhouse.postgres_ext import DateTimeTZField

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
HAS_BTW_NR_KEY = "has_btw_nr"
HAS_KVK_NR = "has_kvk_nr"
KVK_LIST_KEY = "kvk_list"
BTW_LIST_KEY = "btw_list"
POSTCODE_LIST_KEY = "postcode_list"
PAY_OPTION_KEY = "pay_option"
SOCIAL_MEDIA_KEY = "social_media"
REFERRED_KEY = "referred_by"
SUBDOMAIN_KEY = "subdomain"
EXTENSION_KEY = "extension"
SSL_KEY = "ssl"
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

SOCIALMEDIA = [
    "facebook",
    "twitter",
    "whatapp",
    "instagram",
    "youtube"
]

PAY_OPTIONS = [
    "paypall",
    "ideal",
    "visa",
]

logger = logging.getLogger(__name__)


def init_database(database_name: Path,
                  database_type="postgres", user="postgres",
                  password=None, host="localhost", port=5432):
    assert database_type in DATABASE_TYPES
    if database_type == "postgres":
        logger.debug(f"Opening postgres database {database_name}\n"
                     f"user={user}; host={host} port={port}")
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
            legacy_table_names = False

    # this class describes the format of the sql data base
    class Company(BaseModel):
        kvk_nummer = pw.IntegerField(primary_key=True)
        naam = pw.CharField(null=True)
        url = pw.CharField(null=True)
        ranking = pw.IntegerField(null=True)
        core_id = pw.IntegerField(null=True)  # also give the process number. If -1, not done
        datetime = DateTimeTZField(null=True)  # the process time

    class UrlNL(BaseModel):
        """
        Tabel met unieke url's. Een URL kan nul of hooguit 1 kvk nummer hebben, omdat hooguit 1
        bedrijf eigenaar van een url kan zijn. Dit is het verschil met de WebSite tabel, waarbij
        iedere url meerdere kvk's kan hebben omdat dat alleen de kvk zijn die op de site voorkomen,
        maar niet perse de eigenaars van de site.
        """
        # maak url unique, maar gebruik geen primary key voor de url. Dat is minder efficient
        url = pw.CharField(null=True, unique=True)
        bestaat = pw.BooleanField(null=True)
        kvk_nummer = pw.IntegerField(null=True)
        btw_nummer = pw.CharField(null=True)
        datetime = DateTimeTZField(null=True)  # the process time
        ssl = pw.BooleanField(null=True)
        ssl_invalid = pw.BooleanField(null=True)
        subdomain = pw.CharField(null=True)
        domain = pw.CharField(null=True)
        suffix = pw.CharField(null=True)
        category = pw.IntegerField(null=True)
        ecommerce = pw.IntegerField(null=True)
        social_media = pw.CharField(null=True)
        referred_by = pw.CharField(null=True)

    class Address(BaseModel):
        company = pw.ForeignKeyField(Company, backref="address")
        kvk_nummer = pw.IntegerField(null=True)
        naam = pw.CharField(null=True)
        plaats = pw.CharField(null=True)
        postcode = pw.CharField(null=True)
        straat = pw.CharField(null=True)

    class WebSite(BaseModel):
        company = pw.ForeignKeyField(Company, backref="websites")
        url = pw.CharField(null=False)
        naam = pw.CharField(null=False)
        getest = pw.BooleanField(null=True)
        levenshtein = pw.IntegerField(null=True)
        string_match = pw.FloatField(null=True)
        best_match = pw.BooleanField(null=True)
        has_postcode = pw.BooleanField(null=True)
        has_kvk_nr = pw.BooleanField(null=True)
        has_btw_nr = pw.BooleanField(null=True)
        ranking = pw.IntegerField(null=True)
        bestaat = pw.BooleanField(null=True)

    class PayOptions(BaseModel):
        naam = pw.CharField(null=False, unique=True)
        company = pw.ForeignKeyField(Company, backref='pay_options')

    class SocialMedia(BaseModel):
        naam = pw.CharField(null=False, unique=True)
        company = pw.ForeignKeyField(Company, backref='social_media')

    tables = (UrlNL, Company, Address, WebSite, PayOptions, SocialMedia)

    if db.is_closed():
        db.connect()
    if reset_tables and Company.table_exists():
        db.drop_tables(tables)
    db.create_tables(tables)

    return tables

