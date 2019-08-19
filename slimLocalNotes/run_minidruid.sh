#!/usr/bin/env bash
mvn clean test -Dtest=TestMiniDruidLocalCliDriver -Djava.net.preferIPv4Stack=true -Dtest.output.overwrite=true -Dqfile=druidmini_test.q
