#!/usr/bin/python

import sys
from prettyprint.prettyprint import pp

sys.path.insert(0, "../")

object_ring = "object.ring.gz"

from swift.common.ring import Ring, RingData

#ring = Ring(object_ring)
#pp(ring.get_nodes("test_account", "test_container", "test_object"))

ring_data = RingData.load(object_ring)
pp(ring_data.to_dict())


