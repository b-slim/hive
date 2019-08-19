ps -ax | grep io.druid | cut  -d ' ' -f1 | xargs kill -9
