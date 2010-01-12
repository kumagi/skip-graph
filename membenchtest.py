#!/usr/bin/env python
import memcache
print "started"
m1 = memcache.Client(['133.68.129.212:11211'], debug=1)
m2 = memcache.Client(['133.68.129.213:11211'], debug=1)
print "connected"
m1.set("hoge", "fuga")
m1.set("hge", "fuga")
m2.set("h4aoge", "fugfa")
m1.set("hoae", "fuga")
m2.set("h3og", "fuagas")
m1.set("hog7e", "fuga")
m1.set("hfe", "fua2gac")
m2.set("hage", "fugza")
m1.set("hdog", "fua4gas")
m2.set("h3aoge", "fuga")
m1.set("hed", "fus7gfac")
m2.set("h5age5", "fufgza")
m1.set("hcog", "fuvagas")
m2.set("ho51ge", "fuga")
m1.set("he", "fugac")
m1.set("h5ge", "fugza")
m2.set("h12ge", "fucga")
value = m1.get("hoge")
if value == "fuga":
    print ok


