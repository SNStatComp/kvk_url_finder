import peewee as pw

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

# postpone the parsing of the database after we have created the parser class
database = pw.SqliteDatabase(None)


class UnknownField(object):
    def __init__(self, *_, **__): pass


class BaseModel(pw.Model):
    class Meta:
        database = database
        only_save_dirty = True


# this class describes the format of the sql data base
class Company(BaseModel):
    kvk_nummer = pw.IntegerField(primary_key=True)
    naam = pw.CharField(null=True)
    url = pw.CharField(null=True)
    processed = pw.BooleanField(default=False)


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
