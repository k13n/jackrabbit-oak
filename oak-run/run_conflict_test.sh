#!/bin/sh

# drop the test database, which might still exist in case
# the benchmarks (next command) aborted due to some exception
mongo conflicts --eval "db.dropDatabase()" &> /dev/null

java \
  -DclusterSize=2 \
  -Dverbose=false \
  -jar target/oak-run-1.4-SNAPSHOT.jar benchmark \
  --db conflicts \
  --concurrency 2 \
  IndexConflictsProvokingTest IndexConflictsNonProvokingTest Oak-Mongo
