#!/usr/bin/env python
import memcache
print "started"
mc = memcache.Client(['127.0.0.1:11211'], debug=1)
print "connected"
mc.set("hoge", "fuga")
value = mc.get("hoge")
if value == "fuga":
    print ok


