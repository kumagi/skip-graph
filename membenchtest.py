#!/usr/bin/env python
import memcache
import time
print "started"
m1 = memcache.Client(['133.68.129.213:11211'], debug=1)
m2 = memcache.Client(['133.68.129.214:11211'], debug=1)
m2.set("l", "fufgza")
m2.set("p", "fugza")
m2.set("f", "fuga")
m2.set("b", "fuga")
m2.set("n", "fuga")
m2.set("h", "fugza")
m2.set("j", "fuga")
m2.set("d", "fuga")
m1.set("a", "fuga")
m1.set("e", "fuagas")
m1.set("k", "fus7gfac")
m1.set("g", "fua2gac")
m1.set("i", "fua4gas")
m1.set("q", "fucga")
m1.set("m", "fuvagas")
m1.set("c", "fugfa")
m1.set("o", "fugac")
