# -*- coding: utf-8 -*-
import re
import time

import pytest

# XXX: ENDING could be sorted by ocurence frequency
ENDINGS = (u's', u'š', u'is', u'us', u'i', u'a', u'u', u'am', u'im', u'um', u'iem',
           u'ā', u'ī', u'ū', u'os', u'as', u'e', u'es', u'ai', u'ām', u'ei', u'em',
           u'ēm', u'ij', u'īm', u'ās', u'ē', u'ēs', u'īs')

WORD_LIST = [
    u'Sarfas',
    u'Sirmais',
    u'Skrastiņš',
    u'Vanags',
    u'Vasiļjevs',
    u'Vitolins',
    u'Vītols',
    u'Lācis',
    u'Liepa',
    u'Liepiņš',
    u'Dārziņš',
    u'Dukurs',
    u'Dzerins',
    u'Balodis',
    u'Bartulis',
    u'Bertāns',
    u'Bertholds',
    u'Bērziņš',
    u'Briedis',
    u'Ābols',
    u'Alksnis',
    u'Arājs',
    u'Eglītis',
    u'Ezergailis',
]

STEM_LIST = [
    u'Sarfa',
    u'Sirmai',
    u'Skrasti\u0146',
    u'Vanag',
    u'Vasi\u013cjev',
    u'Vitolin',
    u'V\u012btol',
    u'L\u0101ci',
    u'Liep',
    u'Liepi\u0146',
    u'D\u0101rzi\u0146',
    u'Dukur',
    u'Dzerin',
    u'Balodi',
    u'Bartuli',
    u'Bert\u0101n',
    u'Berthold',
    u'B\u0113rzi\u0146',
    u'Briedi',
    u'\u0100bol',
    u'Alksni',
    u'Ar\u0101j',
    u'Egl\u012bti',
    u'Ezergaili',
]


def word_root_re(word):
    word = unicode(word)
    for end in ENDINGS:
        # XXX: maybe using str.endswith is quicker thatn reqgex !?
        stem = re.sub(end + "$", '', word, re.UNICODE)
        if stem != word:
            break

    if not stem:
        stem = word

    return stem


def word_root(word):
    for end in ENDINGS:
        if word.endswith(end) and word != end:
            return word[:-len(end)]
    else:
        return word


@pytest.mark.parametrize('word,stem', zip(WORD_LIST, STEM_LIST))
def test_words_root_steming(word, stem):
    assert word_root(word) == stem


@pytest.mark.parametrize('word,stem', zip(WORD_LIST, STEM_LIST))
def test_words_root_re_steming(word, stem):
    assert word_root_re(word) == stem


def word_root_speed(method, word_seq):
    start = time.time()
    for word in word_seq:
        method(word)
    end = time.time()
    return end - start


def test_speed_comparison():
    no_regex =  word_root_speed(word_root, WORD_LIST)
    regex = word_root_speed(word_root_re, WORD_LIST)

    assert no_regex < regex
    assert regex / no_regex > 5
