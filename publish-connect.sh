#! /usr/bin/env bash

if [[ "$#" -ne 1 ]]; then
  echo "Usage: $0 <version>"
  exit 1
fi

set -e

VERSION=4.3.0-chrise-$1-uber

./gradlew clean
./gradlew updateVersion -PnewVersion=$VERSION
./gradlew build -x test -x rat -x spotbugsMain -x spotbugsTest -x checkstyleMain -x checkstyleTest
./gradlew releaseTarGz
./gradlew publish -x test -x rat \
  -PmavenUrl=https://artifacts.uberinternal.com/artifactory/libs-release-local \
  -PmavenPassword="$(usso -ussh artifacts.uberinternal.com -print)" \
  -PmaxParallelForks=8 --max-workers=8 \
  -x :streams:publish \
  -x :streams:examples:publish \
  -x :streams:test-utils:publish \
  -x :streams:streams-scala:publish \
  -x :streams:integration-tests:publish \
  -x :streams:upgrade-system-tests-0110:publish \
  -x :streams:upgrade-system-tests-10:publish \
  -x :streams:upgrade-system-tests-11:publish \
  -x :streams:upgrade-system-tests-20:publish \
  -x :streams:upgrade-system-tests-21:publish \
  -x :streams:upgrade-system-tests-22:publish \
  -x :streams:upgrade-system-tests-23:publish \
  -x :streams:upgrade-system-tests-24:publish \
  -x :streams:upgrade-system-tests-25:publish \
  -x :streams:upgrade-system-tests-26:publish \
  -x :streams:upgrade-system-tests-27:publish \
  -x :streams:upgrade-system-tests-28:publish \
  -x :streams:upgrade-system-tests-30:publish \
  -x :streams:upgrade-system-tests-31:publish \
  -x :streams:upgrade-system-tests-32:publish \
  -x :streams:upgrade-system-tests-33:publish \
  -x :streams:upgrade-system-tests-34:publish \
  -x :streams:upgrade-system-tests-35:publish \
  -x :streams:upgrade-system-tests-36:publish \
  -x :streams:upgrade-system-tests-37:publish \
  -x :streams:upgrade-system-tests-38:publish \
  -x :streams:upgrade-system-tests-39:publish \
  -x :streams:upgrade-system-tests-40:publish \
  -x :streams:upgrade-system-tests-41:publish
curl -H "Authorization: Bearer $(usso -ussh artifacts -print)" \
  https://artifacts.uberinternal.com/artifactory/libs-release-local/org/apache/kafka/kafka_2.13/$VERSION/kafka_2.13-$VERSION.tgz \
  -T core/build/distributions/kafka_2.13-$VERSION.tgz
