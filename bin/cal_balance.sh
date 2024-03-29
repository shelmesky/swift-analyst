#!/bin/bash



rm -f *.builder *.ring.gz backups/*.builder backups/*.ring.gz
python2 ./swift-ring-builder object.builder create 4 3 1
python2 ./swift-ring-builder object.builder add z1-127.0.0.1:6010/sdb1 1
python2 ./swift-ring-builder object.builder add z2-127.0.0.1:6020/sdb2 1
python2 ./swift-ring-builder object.builder add z3-127.0.0.1:6030/sdb3 1
python2 ./swift-ring-builder object.builder add z4-127.0.0.1:6040/sdb4 1
python2 ./swift-ring-builder object.builder rebalance

#swift-ring-builder container.builder create 18 3 1
#swift-ring-builder container.builder add z1-127.0.0.1:6011/sdb1 1
#swift-ring-builder container.builder add z2-127.0.0.1:6021/sdb2 1
#swift-ring-builder container.builder add z3-127.0.0.1:6031/sdb3 1
#swift-ring-builder container.builder add z4-127.0.0.1:6041/sdb4 1
#swift-ring-builder container.builder rebalance
#swift-ring-builder account.builder create 18 3 1
#swift-ring-builder account.builder add z1-127.0.0.1:6012/sdb1 1
#swift-ring-builder account.builder add z2-127.0.0.1:6022/sdb2 1
#swift-ring-builder account.builder add z3-127.0.0.1:6032/sdb3 1
#swift-ring-builder account.builder add z4-127.0.0.1:6042/sdb4 1
#swift-ring-builder account.builder rebalance
