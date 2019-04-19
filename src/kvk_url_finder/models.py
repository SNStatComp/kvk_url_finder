from pathlib import Path
import logging

import peewee as pw
from playhouse.pool import (PooledPostgresqlExtDatabase)
from playhouse.postgres_ext import DateTimeTZField

KVK_KEY = "kvk_nummer"
BTW_KEY = "btw_nummer"
NAME_KEY = "naam"
URL_KEY = "url"
URL_ID_KEY = "url_id"
CORE_ID = "core_id"
URLNL_KEY = "url_nl"
ADDRESS_KEY = "straat"
POSTAL_CODE_KEY = "postcode"
CITY_KEY = "plaats"
COMPANY_KEY = "company"
COMPANY_ID_KEY = "company_id"
BEST_MATCH_KEY = "best_match"
STRING_MATCH_KEY = "string_match"
RANKING_KEY = "ranking"
MAX_PROCESSES = 128
MAX_CHARFIELD_LENGTH = 4095

GETEST_KEY = "getest"
BESTAAT_KEY = "bestaat"
EXISTS_KEY = "exists"
DISTANCE_KEY = "distance"
URL_MATCH = "url_match"
URL_RANK = "url_rank"
DISTANCE_STRING_MATCH_KEY = "dist_str_match"
HAS_POSTCODE_KEY = "has_postcode"
HAS_BTW_NR_KEY = "has_btw_nr"
HAS_KVK_NR = "has_kvk_nr"
KVK_LIST_KEY = "kvk_list"
BTW_LIST_KEY = "btw_list"
POSTCODE_LIST_KEY = "postcode_list"
SOCIAL_MEDIA_KEY = "social_media"
ECOMMERCE_KEY = "ecommerce"
REFERRED_KEY = "referred_by"
SUBDOMAIN_KEY = "subdomain"
EXTENSION_KEY = "extension"
SSL_KEY = "ssl"
DOMAIN_KEY = "domain"
SUFFIX_KEY = "suffix"
CATEGORY_KEY = "category"
UPDATE_KEY = "update"

ALL_KVK_KEY = "all_kvk"
ALL_BTW_KEY = "all_btw"
ALL_PSC_KEY = "all_psc"
DATETIME_KEY = "datetime"
CHECKED_KEY = "getest"

SSL_KEY = "ssl"
SSL_VALID_KEY = "ssl_valid"


WEB_DF_COLS = [URL_KEY,
               EXISTS_KEY,
               DISTANCE_KEY,
               STRING_MATCH_KEY,
               DISTANCE_STRING_MATCH_KEY,
               URL_RANK,
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

SOCIAL_MEDIA = [
    "Facebook",
    "YouTube",
    "WhatsApp",
    "Messenger",
    "WeChat",
    "Instagram",
    "Twitter",
    "Skype",
    "LinkedIn",
    "Viber",
    "Snapchat",
    "Line",
    "Pinterest",
    "Telegram",
    "Tinder",
]

PAY_OPTIONS = [
    "PayPal",
    "PaymentWall",
    "Ideal",
    "GooglePay",
    "GoogleWallet",
    "CreditCard",
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
        url = pw.CharField(null=True, unique=True, primary_key=True)
        bestaat = pw.BooleanField(null=True)
        post_code = pw.CharField(null=True)
        kvk_nummer = pw.IntegerField(null=True)
        btw_nummer = pw.CharField(null=True)
        datetime = DateTimeTZField(null=True)  # the process time
        ssl = pw.BooleanField(null=True)
        ssl_invalid = pw.BooleanField(null=True)
        subdomain = pw.CharField(null=True)
        domain = pw.CharField(null=True)
        suffix = pw.CharField(null=True)
        category = pw.IntegerField(null=True)
        ecommerce = pw.CharField(null=True, max_length=MAX_CHARFIELD_LENGTH)
        social_media = pw.CharField(null=True, max_length=MAX_CHARFIELD_LENGTH)
        referred_by = pw.CharField(null=True)
        all_psc = pw.CharField(null=True, max_length=MAX_CHARFIELD_LENGTH)
        all_kvk = pw.CharField(null=True, max_length=MAX_CHARFIELD_LENGTH)
        all_btw = pw.CharField(null=True, max_length=MAX_CHARFIELD_LENGTH)

    class Address(BaseModel):
        company = pw.ForeignKeyField(Company, backref="address")
        kvk_nummer = pw.IntegerField(null=True)
        naam = pw.CharField(null=True)
        plaats = pw.CharField(null=True)
        postcode = pw.CharField(null=True)
        straat = pw.CharField(null=True)

    class WebSite(BaseModel):
        company = pw.ForeignKeyField(Company, backref="websites")
        # url = pw.CharField(null=False)
        url = pw.ForeignKeyField(UrlNL, backref="websites")
        naam = pw.CharField(null=False)
        getest = pw.BooleanField(null=True)
        bestaat = pw.BooleanField(null=True)
        levenshtein = pw.IntegerField(null=True)
        string_match = pw.FloatField(null=True)
        url_match = pw.FloatField(null=True)
        url_rank = pw.FloatField(null=True)
        best_match = pw.BooleanField(null=True)
        has_postcode = pw.BooleanField(null=True)
        has_kvk_nr = pw.BooleanField(null=True)
        has_btw_nr = pw.BooleanField(null=True)
        ranking = pw.FloatField(null=True)

    tables = (UrlNL, Company, Address, WebSite)

    if db.is_closed():
        db.connect()
    if reset_tables and Company.table_exists():
        db.drop_tables(tables)
    db.create_tables(tables)

    return tables
