#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
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

import hashlib
import re
import sys
import os
import unicodedata

from datetime import datetime

import MySQLdb as mdb

# Stop encoding bullshit in python 2.x
# 3.x finally fixes this
reload(sys)
sys.setdefaultencoding("utf-8")

DB_URL = os.environ.get('DATABASE_URL', 'mysql://user:password@localhost/db')

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

from collections import namedtuple

Entry = namedtuple('Entry', ['id', 'text'])

Criteria = namedtuple(
    'Criteria',
    ['id', 'user_id', 'pub_at', 'name', 'surname'],
)


def db_connect(db_url):
    try:
        connection = mdb.connect(db_url)
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.exception('Error ({}) connecting to DB: {}'.format(type(e), e))
        raise RuntimeError('Failed to create db connection')

    return connection


def MatcherDelete(conn, criteria_id, match_table):
    """Deletes Matched entries with user filters.

    :param conn: Database connection.
    :param int criteria_id: Criteria ID in database.
    :param str match_table: Database table name to use to get match.

    """
    query_str = 'DELETE FROM {} WHERE (%s)=(%s)'.format(match_table)
    cur = conn.cursor()
    cur.execute(query_str, ('criterion_id', int(criteria_id)))


class Matcher(object):
    """Matches entries with user criteria."""

    def __init__(self, conn, user_id=None, criteria_id=None, all_dates=0):
        self._conn = conn
        self.db = self._conn.cursor()

        # XXX: why we need this comment?
        # XXX: Is this going to happen on each match?
        # Read all in memory
        self.find_matching(
            criterias=self._fetch_criteria(CRITERIA_TABLE, user_id, criteria_id),
            entries=self._fetch_entries(ENTRY_TABLE, all_dates),
        )

    def _fetch_criteria(self, table, user_id, criteria_id):
        query_str = 'SELECT * FROM {table}'
        where_cuase_list = []

        if user_id:
             where_cuase_list.append('app_criterion.user_id = {user_id}')

        if criteria_id:
            where_cuase_list.append('app_criterion.id = {criteria_id}')

        if where_cuase_list:
            where_cause = ' AND '.join(where_cuase_list)
            query_str = ' WHERE '.join([query_str, where_cause])

        self.db.execute(
            query_str.format(
                table=table,
                user_id=user_id,
                criteria_id=criteria_id,
            ),
        )
        return [Criteria(*row[:5]) for row in self.db.fetchall()]

    def _fetch_entries(self, table, all_dates):
        if all_dates:
            query_str = 'SELECT id, entry_text FROM {table}'
        else:
            query_str = """SELECT id, entry_text FROM {table}
                    WHERE DATEDIFF({table}.pub_date, '{now}') = 0"""

        self.db.execute(
            query_str.format(
                table=table,
                now=datetime.utcnow().date(),
            ),
        )
        return [Entry(*row[:3]) for row in self.db.fetchall()]

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
        for word in criteria.name.lower().split():
            # 2) Get root of each word
            all_words.append(self._new_normalize(self._get_word_root(word)))

        for word in criteria.surname.lower().split():
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
    # XXX: add connection as context manager that finishes trasaction.
    Matcher(db_connect(DB_URL))
