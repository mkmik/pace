#!/usr/bin/env python

import pymongo
import bitstring

from hashes.simhash import simhash


connection = pymongo.Connection("localhost", 27017)

db = connection.pace
coll = db.people

for i in  coll.find():
    h = simhash(i['firstName'] + i['lastName'], hashbits=64)
    
    ha = bitstring.pack('uint:64', long(h))
    for j in xrange(0, 8):
        i['h%s' % j] = ha.hex
        #print ha.hex
        ha.ror(64 / 8)

    coll.update({'n': i['n']}, i)
