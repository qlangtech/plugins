#rsync --include="dolphinscheduler-task-tis-datasync-*.jar"  -vr ./target/* root@192.168.28.201:/opt/misc/apache-dolphinscheduler-3.2.2-bin/libs/
scp ../tis-datax-executor.tar.gz root@192.168.28.201:/opt/misc/apache-dolphinscheduler-3.2.2-bin/standalone-server/
