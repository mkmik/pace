#!/usr/bin/env python

import pymongo
import bitstring

from hashes.simhash import simhash


connection = pymongo.Connection("localhost", 27017)

db = connection.pace
coll = db.people

bits = 64

for i in  coll.find():
    h = simhash(i['firstName'] + i['lastName'], hashbits=bits)
    
    ha = bitstring.pack('uint:%s' % bits, long(h))
    for j in xrange(0, bits):
        #i['h%s' % j] = ha.hex
        #print ha.hex
        print '%s:%s' % (ha.uint, i['n'])
        ha.ror(bits / bits)

    #coll.update({'n': i['n']}, i)
