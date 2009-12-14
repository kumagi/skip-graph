#!/usr/bin/env python
import memcache
print "started"
mc = memcache.Client(['127.0.0.1:11211'], debug=1)
print "connected"
mc.set("hoge", "fuga")
mc.set("hge", "fuga")
mc.set("h4aoge", "fugfa")
mc.set("hoae", "fuga")
mc.set("h3og", "fuagas")
mc.set("hog7e", "fuga")
mc.set("hfe", "fua2gac")
mc.set("hge", "fugza")
mc.set("hdog", "fua4gas")
mc.set("h3aoge", "fuga")
mc.set("hed", "fus7gfac")
mc.set("h5age5", "fufgza")
mc.set("hcog", "fuvagas")
mc.set("ho51ge", "fuga")
mc.set("he", "fugac")
mc.set("h5ge", "fugza")
mc.set("hge", "fucga")
value = mc.get("hoge")
if value == "fuga":
    print ok


