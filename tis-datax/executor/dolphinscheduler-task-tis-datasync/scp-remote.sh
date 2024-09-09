#rsync --include="dolphinscheduler-task-tis-datasync-*.jar"  -vr ./target/* root@192.168.28.201:/opt/misc/apache-dolphinscheduler-3.2.2-bin/libs/
scp ./target/dolphinscheduler-task-tis-datasync-4.0.1-shaded.jar root@192.168.28.201:/opt/misc/apache-dolphinscheduler-3.2.2-bin/libs/dolphinscheduler-task-datasync-3.2.2.jar
