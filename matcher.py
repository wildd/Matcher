#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
# XXX: Use correct shabang string
# XXX: Use correct encoding string
"""
High Level suggestions:
1. Leverage database search capabilities
 - MySQL full-text search
 - query database instead of iterating over results
2. Cache intermediate results in database
3. Use regexes sparingly
4. Use more appropriate database like Elastic Search


TODO
====
- Tests (there are none)
Method Arguments
Null Pointers
Naming and style conversions
Language Idioms
Duplication
Security
"""

# XXX: PEP8 import ordering
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

# XXX: useless comment is useless
# XXX: use DB URI strings (following 12factor.net)
# db
DB_ARGS = {
    'host': 'localhost',
    'user': 'user',
# XXX: Keep passwords and sensitive information separate from code
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

# XXX: Need more context to understand naming and meaning
WORD_START = r'(\b'
WORD_END = r'\w*\b)'
WORD_END_STRICT = r'\b)'
WORD_SEPARATOR = r'\W*'
WORD_START_SIMPLE = r'('
WORD_END_SIMPLE = r')'
WORD_SEPARATOR_SIMPLE = r'\s+'

# XXX: Turn into named tuple, because there are no methods. It's simple value
# object
class Entry(object):
    def __init__(self, id, text):
        self.id = id
        self.text = text

# XXX: Turn into named tuple.
# XXX: Use unpacking to pass arguments!?
class Criteria(object):
    def __init__(self, row):
        self.id = row[0]
        self.user_id = row[1]
        self.pub_date = row[2]
        self.criterion_name = row[3]
        self.criterion_surname = row[4]


# XXX: docstrings standards https://www.python.org/dev/peps/pep-0257/
class MatcherDelete(object):
    '''
    Deletes Matched entries with user filters
    '''

    # XXX: use snake_case for variable names and arguments
    def __init__(self, criteriaId):
        # XXX: connection to database should be passed in as argument
        self._connect_db()

        self._delete_matching(criteriaId)
        # XXX: Uses hidden state self.connection. This would fail if
        # self._connect_db wouldn't be called
        # XXX: Use context managers to manage transactions, file opening etc
        self.connection.commit()
        self.connection.close()

    def _connect_db(self):
        try:
            # XXX: Using global argument inside method
            self.connection = mdb.connect(**DB_ARGS)
        except Exception as e:
            # XXX: Use logging framework for logging
            print("Error ({}) connecting to DB: {}".format(type(e), e))
            # XXX: Create domain specific exception.
            # XXX: Keep original exception as context 
            raise RuntimeError("Failed to create db connection")

        # XXX: creating state, that is used only one place
        self.db = self.connection.cursor()

    def _delete_matching(self, criteriaId):
        # XXX: security. Don't interpolate SQL query arguments. Pass them in as
        # second parameter self.db.execute(query_str, query_args)
        # XXX: MATCH_TABLE global variable in method
        # XXX: Use consistent string quotations
        self.db.execute('DELETE FROM {} WHERE (%s)=(%s)'.format(MATCH_TABLE) % ('criterion_id', int(criteriaId)))


class Matcher(object):
    '''
    Matches entries with user criteria
    '''
    # XXX: use snake_case
    def __init__(self, userId=None, criteriaId=None, allDates=None):
        # XXX: pass in database connection
        self._connect_db()

        if allDates is None:
            allDates = 0

        # XXX: why we need this comment?
        # XXX: Is this going to happen on each match?
        # Read all in memory
        criterias = self._fetch_criteria(CRITERIA_TABLE, userId, criteriaId)
        entries = self._fetch_entries(ENTRY_TABLE, allDates)

        # XXX: why we don't inline method calls?
        self.find_matching(criterias, entries)
        self.connection.commit()
        self.connection.close()

    # XXX: duplication as MatcherDelete._connect_db
    def _connect_db(self):
        try:
            self.connection = mdb.connect(**DB_ARGS)
        except Exception as e:
            print("Error ({}) connecting to DB: {}".format(type(e), e))
            raise RuntimeError("Failed to create db connection")

        self.db = self.connection.cursor()

    # XXX: use snake_case
    def _fetch_criteria(self, table, userId, criteriaId):
        # XXX: too far from usage
        criteriaList = []

        # XXX: Nested ifs. Remove ifs as much as possible.
        if userId:
            if criteriaId:
                # XXX: don't interpolate SQL query. Use mogrify function provided
                # by API. That takes care of some interpolation mistakes
                # XXX: Query string consistency. Use ALL CAPS everywhere
                sql = "select * from {}\
                where app_criterion.user_id = {} and app_criterion.id = {}".format(table, userId, criteriaId)
            else:
                sql = "select * from {}\
                where app_criterion.user_id = {}".format(table, userId)
        else:
            sql = "select * from {}".format(table)

        self.db.execute(sql)
        rows = self.db.fetchall()

        # XXX: use list comprehension
        for row in rows:
            criteriaList.append(Criteria(row))

        # XXX: return directly list comprehension
        return criteriaList

    # XXX: Use snake_case 
    def _fetch_entries(self, table, allDates):
        # XXX: too far from usage point
        entriesList = []

        # XXX: use mogrify
        if allDates:
            sql = "select id, entry_text from {}".format(table)
        else:
            sql = "select id, entry_text from {}\
                    where DATEDIFF({}.pub_date, '{}') = 0".format(table, table, datetime.utcnow().date())

        self.db.execute(sql)
        rows = self.db.fetchall()

        # XXX: Use list comprehensions
        for row in rows:
            # XXX: maybe use unpacking
            entriesList.append(Entry(row[0], row[1]))

        return entriesList

    def _add_criterion_entry(self, criterion_id, entry_id):
        # Check criterion-entry connection doesn't exist in DB already
        # XXX: don't interpolate values of SQL query
        self.db.execute("SELECT * FROM {} WHERE criterion_id = {} and entry_id = {}".format(MATCH_TABLE, criterion_id, entry_id)) 
        # XXX: inline
        row = self.db.fetchone()
        # XXX: maybe explicit else would be required
        if row is None:
            # XXX: pull out in separete function
            # Create unique hash link for every criterion-entry connection
            link = hashlib.sha1("/criterions/" + str(criterion_id) + "/entry/" + str(entry_id) + "/").hexdigest()
            # Add criterion-entry connection
            self.db.execute("INSERT INTO {} (criterion_id, entry_id, link) VALUES(%s, %s, %s)".format(MATCH_TABLE), (criterion_id, entry_id, link))

    # XXX: this isn't part of Matcher's responsiblities. Pull out as separeate
    # function
    def _get_word_root(self, word):
        # XXX: reusing word name
        eord = unicode(word)
        # Cut off all possible endings
        for end in ENDINGS:
            # XXX: new_word is called stem
            # XXX: ENDING could be sorted by ocurence frequency
            # XXX: maybe using str.endswith is quicker thatn reqgex !?
            new_word = re.sub(end + "$", '', word, re.UNICODE)
            if new_word != word:
                break
        # XXX: use idomatic python. if not new_word
        if new_word == "":
            new_word = word

        return new_word

    # text without diacritical chars
    # XXX: This isn't par to Matcher responsiblilities
    # XXX: What does _new_normalize means?
    def _new_normalize(self, text):
        # XXX: can be inlined
        text = unicode(text)

        text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore')

        return text

    def _create_person_regex(self, criteria):

        all_words = []

        # 1) Split names and surnames in multiple words
        # XXX: pull out normalization part
        # XXX: Why it's isn't normalized on the spot? It's only place it's used.
        # XXX: merge two loops in one
        # XXX: use list comprehension
        for word in criteria.criterion_name.lower().split():
            # 2) Get root of each word
            all_words.append(self._new_normalize(self._get_word_root(word)))

        for word in criteria.criterion_surname.lower().split():
            all_words.append(self._new_normalize(self._get_word_root(word)))

        if not all_words:
            # If there were no words...
            # XXX: potential null pointer
            return

        # Create result regex
        # XXX: use list comprehension
        regex_list = []
        for word_root in all_words:
            # XXX: using globals from method
            # XXX: maybe there is separate responsibility MatcherRegexBuilder!?
            regex_list.append(WORD_START + word_root + WORD_END)

        # XXX: use VCS to keep history
        #print("regex list: {}".format(regex_list))
        regex = WORD_SEPARATOR.join(regex_list)
        return regex

    # XXX: public method after private methods
    def find_matching(self, criterias, entries):
        for criteria in criterias:
            # XXX: keeping state
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
                    # XXX: move out of for loop
                    match_regex = self._create_person_regex(criteria)
                    first = False

                # Matching
                if match_regex:
                    # XXX: normalization should be done soon as possible.
                    # Guard your inputs, avoid double checking down the
                    # abstraction layers
                    # XXX: Store entry normalized
                    matched = re.search(match_regex, self.new_normalize(entry.text.lower()), re.UNICODE)

                # If matched, create connection @ DB
                # XXX: Explicit else!?
                if matched:
                    self._add_criterion_entry(criteria.id, entry.id)

if __name__ == "__main__":
    # XXX: all module running shebang
    Matcher()
