from pathlib import Path

import peewee as pw
from playhouse.pool import (PooledPostgresqlExtDatabase)

KVK_KEY = "kvk_nummer"
NAME_KEY = "naam"
URL_KEY = "url"
ADDRESS_KEY = "straat"
POSTAL_CODE_KEY = "postcode"
CITY_KEY = "plaats"
COMPANY_KEY = "company"
BEST_MATCH_KEY = "best_match"
LEVENSHTEIN_KEY = "levenshtein"
STRING_MATCH_KEY = "string_match"
RANKING_KEY = "ranking"
MAX_PROCESSES = 128
PRAGMAS = {
    "journal_mode": "wal",
    "foreingn_keys": 1,
    "ignore_check_constraints": 0,
    "synchronous": 0
}
DATABASE_TYPES = ("sqlite", "postgres")


def init_database(database_name: Path, database_type="postgres"):
    assert database_type in DATABASE_TYPES
    if database_type == "postgres":
        db = PooledPostgresqlExtDatabase(
            database_name, user="postgres", host="localhost", port=5432, password="vliet123",
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
        url = pw.CharField(null=True)
        process_nr = pw.IntegerField(default=-1)  # also give the process number. If -1, not done
        process_time = pw.DateTimeField(null=True)  # the process time

    class Address(BaseModel):
        company = pw.ForeignKeyField(Company, backref="address")
        naam = pw.CharField(null=True)
        plaats = pw.CharField(null=True)
        postcode = pw.CharField(null=True)
        straat = pw.CharField(null=True)

    class WebSite(BaseModel):
        company = pw.ForeignKeyField(Company, backref="websites")
        url = pw.CharField(null=False)
        naam = pw.CharField(null=False)
        getest = pw.BooleanField(default=False)
        levenshtein = pw.IntegerField(default=-1)
        string_match = pw.FloatField(default=-1)
        best_match = pw.BooleanField(default=True)
        ranking = pw.IntegerField(default=-1)
        bestaat = pw.BooleanField(default=False)

    tables = (Company, Address, WebSite)

    if db.is_closed():
        db.connect()
    if reset_tables and Company.table_exists():
        db.drop_tables(tables)
    db.create_tables(tables)

    return tables
