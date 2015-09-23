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
5. Cache as much as possible

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
import logging

from itertools import chain
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

# XXX: ENDING could be sorted by ocurence frequency
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
        # XXX: Add in memmory caching
        # Read all in memory
        self.find_matching(
            criterias=self._fetch_criteria(CRITERIA_TABLE, user_id, criteria_id),
            entries=self._fetch_entries(ENTRY_TABLE, all_dates),
        )

    def find_matching(self, criterias, entries):
        """
        1) Check if name/surname has endings if has, cut them off
        # kristaps -> kristap; kristapam -> kristap; utt
        2) Next we create regex for name+space+name+..
        # "kristaps"; "berzins" -> "kristap"; "berzin" -> "kristap berzin"

        We should have matching against this text and similar texts
        "Šodien Kristapam Bērziņam ir jāiet uz darbu."
        """
        for criteria in criterias:
            criteria_regex = self._create_person_regex(criteria)
            for entry in entries:
                # XXX: normalization should be done soon as possible.
                # Guard your inputs, avoid double checking down the
                # abstraction layers
                # XXX: Store entry normalized
                matched = re.search(
                    criteria_regex,
                    normalize_ascii(entry.text.lower()),
                    flags=re.UNICODE,
                )

                # If matched, create connection @ DB
                # XXX: Explicit else!?
                if matched:
                    self._add_criterion_entry(criteria.id, entry.id)

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

        logging.getLogger(__name__).debug(query_str)

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
        """Create unique hash link for every criterion-entry connection."""
        # Check criterion-entry connection doesn't exist in DB already
        self.db.execute("""SELECT * FROM {}
                WHERE criterion_id = %s AND entry_id = %s""".format(MATCH_TABLE), 
                (criterion_id, entry_id),
        )

        if not self.db.fetchone():
            # Create unique hash link for every criterion-entry connection
            store_connection(self.db, entry_id, criterion_id)
            path = '/criterions/{criterion_id}/entry/{entry_id}/'.format(
                criterion_id=criterion_id,
                entry_id=entry_id,
            )
            link = hashlib.sha1(path).hexdigest()
            self.db.execute(
                """INSERT INTO {} (criterion_id, entry_id, link)
                VALUES(%s, %s, %s)""".format(MATCH_TABLE),
                (criterion_id, entry_id, link),
            )

    def _create_person_regex(self, criteria):
        """ Split names and surnames in multiple words.

        :returns: Regex string or None when there is no matches

        """
        # XXX: pull out normalization part
        # XXX: Why it's isn't normalized on the spot? It's only place it's used.
        all_words = [normalize_ascii(word_root(word))
                     for word in chain(criteria.name.lower().split(),
                                       criteria.surname.lower().split())]
        regex_list = [WORD_START + word_root + WORD_END
                      for word_root in all_words]
        logging.getLogger(__name__).debug('regex list: %s', regex_list)
        regex = WORD_SEPARATOR.join(regex_list)
        return regex if regex else None


def word_root(word):
    for end in ENDINGS:
        if word.endswith(end) and word != end:
            return word[:-len(end)]
    else:
        return word


def normalize_ascii(text):
    return (unicodedata.normalize('NFKD', unicode(text))
            .encode('ascii', 'ignore'))


if __name__ == "__main__":
    # XXX: add connection as context manager that finishes trasaction.
    Matcher(db_connect(DB_URL))
