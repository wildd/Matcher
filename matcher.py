#!/home/kristaps/virtualenv/mysite/2.7/bin/python2.7
# coding=utf-8

import sys
# Stop encoding bullshit in python 2.x
# 3.x finally fixes this
reload(sys)
sys.setdefaultencoding("utf-8")

import MySQLdb as mdb
import re
import hashlib
import unicodedata
from datetime import datetime

# db
DB_ARGS = {
    'host': 'localhost',
    'user': 'user',
    'passwd': 'password',
    'db': 'app',
    'charset': 'utf8',
    'use_unicode': 'True'
}

CRITERIA_TABLE = 'app_criterion'
'''
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `pub_date` datetime NOT NULL,
  `criterion_name` varchar(255) NOT NULL,
  `criterion_surname` varchar(255) NOT NULL,

'''
ENTRY_TABLE = 'app_entry'
'''
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `text` longtext NOT NULL,
'''
MATCH_TABLE = 'app_criterionentry'
'''
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `criterion_id` int(11) NOT NULL,
  `entry_id` int(11) NOT NULL,
'''

ENDINGS = (u's', u'š', u'is', u'us', u'i', u'a', u'u', u'am', u'im', u'um', u'iem',
           u'ā', u'ī', u'ū', u'os', u'as', u'e', u'es', u'ai', u'ām', u'ei', u'em',
           u'ēm', u'ij', u'īm', u'ās', u'ē', u'ēs', u'īs')

WORD_START = r'(\b'
WORD_END = r'\w*\b)'
WORD_END_STRICT = r'\b)'
WORD_SEPARATOR = r'\W*'
WORD_START_SIMPLE = r'('
WORD_END_SIMPLE = r')'
WORD_SEPARATOR_SIMPLE = r'\s+'


class Entry(object):
    def __init__(self, id, text):
        self.id = id
        self.text = text


class Criteria(object):
    def __init__(self, row):
        self.id = row[0]
        self.user_id = row[1]
        self.pub_date = row[2]
        self.criterion_name = row[3]
        self.criterion_surname = row[4]


class MatcherDelete(object):
    '''
    Deletes Matched entries with user filters
    '''

    def __init__(self, criteriaId):
        self._connect_db()

        self._delete_matching(criteriaId)
        self.connection.commit()
        self.connection.close()

    def _connect_db(self):
        try:
            self.connection = mdb.connect(**DB_ARGS)
        except Exception as e:
            print("Error ({}) connecting to DB: {}".format(type(e), e))
            raise RuntimeError("Failed to create db connection")

        self.db = self.connection.cursor()

    def _delete_matching(self, criteriaId):
        self.db.execute('DELETE FROM {} WHERE (%s)=(%s)'.format(MATCH_TABLE) % ('criterion_id', int(criteriaId)))


class Matcher(object):
    '''
    Matches entries with user criteria
    '''
    def __init__(self, userId=None, criteriaId=None, allDates=None):
        self._connect_db()

        if allDates is None:
            allDates = 0

        # Read all in memory
        criterias = self._fetch_criteria(CRITERIA_TABLE, userId, criteriaId)
        entries = self._fetch_entries(ENTRY_TABLE, allDates)

        self.find_matching(criterias, entries)
        self.connection.commit()
        self.connection.close()

    def _connect_db(self):
        try:
            self.connection = mdb.connect(**DB_ARGS)
        except Exception as e:
            print("Error ({}) connecting to DB: {}".format(type(e), e))
            raise RuntimeError("Failed to create db connection")

        self.db = self.connection.cursor()

    def _fetch_criteria(self, table, userId, criteriaId):
        criteriaList = []

        if userId:
            if criteriaId:
                sql = "select * from {}\
                where app_criterion.user_id = {} and app_criterion.id = {}".format(table, userId, criteriaId)
            else:
                sql = "select * from {}\
                where app_criterion.user_id = {}".format(table, userId)
        else:
            sql = "select * from {}".format(table)

        self.db.execute(sql)
        rows = self.db.fetchall()

        for row in rows:
            criteriaList.append(Criteria(row))

        return criteriaList

    def _fetch_entries(self, table, allDates):
        entriesList = []

        if allDates:
            sql = "select id, entry_text from {}".format(table)
        else:
            sql = "select id, entry_text from {}\
                    where DATEDIFF({}.pub_date, '{}') = 0".format(table, table, datetime.utcnow().date())

        self.db.execute(sql)
        rows = self.db.fetchall()

        for row in rows:
            entriesList.append(Entry(row[0], row[1]))

        return entriesList

    def _add_criterion_entry(self, criterion_id, entry_id):
        # Check criterion-entry connection doesn't exist in DB already
        self.db.execute("SELECT * FROM {} WHERE criterion_id = {} and entry_id = {}".format(MATCH_TABLE, criterion_id, entry_id))
        row = self.db.fetchone()
        if row is None:
            # Create unique hash link for every criterion-entry connection
            link = hashlib.sha1("/criterions/" + str(criterion_id) + "/entry/" + str(entry_id) + "/").hexdigest()
            # Add criterion-entry connection
            self.db.execute("INSERT INTO {} (criterion_id, entry_id, link) VALUES(%s, %s, %s)".format(MATCH_TABLE), (criterion_id, entry_id, link))

    def _get_word_root(self, word):
        word = unicode(word)
        # Cut off all possible endings
        for end in ENDINGS:
            new_word = re.sub(end + "$", '', word, re.UNICODE)
            if new_word != word:
                break

        if new_word == "":
            new_word = word

        return new_word

    # text without diacritical chars
    def _new_normalize(self, text):
        text = unicode(text)

        text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore')

        return text

    def _create_person_regex(self, criteria):

        all_words = []

        # 1) Split names and surnames in multiple words
        for word in criteria.criterion_name.lower().split():
            # 2) Get root of each word
            all_words.append(self._new_normalize(self._get_word_root(word)))

        for word in criteria.criterion_surname.lower().split():
            all_words.append(self._new_normalize(self._get_word_root(word)))

        if not all_words:
            # If there were no words...
            return

        # Create result regex
        regex_list = []
        for word_root in all_words:
            regex_list.append(WORD_START + word_root + WORD_END)

        #print("regex list: {}".format(regex_list))
        regex = WORD_SEPARATOR.join(regex_list)
        return regex

    def find_matching(self, criterias, entries):
        for criteria in criterias:
            first = True
            for entry in entries:
                matched = False
                # 1) Check if name/surname has endings
                # if has, cut them off
                # kristaps -> kristap; kristapam -> kristap; utt
                # 2) Next we create regex for name+space+name+..
                # "kristaps"; "berzins" -> "kristap"; "berzin" -> "kristap berzin"
                # We should have matching against this text and similar texts
                # "Šodien Kristapam Bērziņam ir jāiet uz darbu."

                # Create regex only first time we have unique criteria
                if first:
                    match_regex = self._create_person_regex(criteria)
                    first = False

                # Matching
                if match_regex:
                    matched = re.search(match_regex, self.new_normalize(entry.text.lower()), re.UNICODE)

                # If matched, create connection @ DB
                if matched:
                    self._add_criterion_entry(criteria.id, entry.id)

if __name__ == "__main__":
    Matcher()
