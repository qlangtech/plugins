rsync  --exclude=".git" --exclude="*.jar" --exclude="*.tpi" --exclude="*.tar.gz" --exclude="*.class" --delete  -vr ../plugins/* root@192.168.28.201:/opt/data/tiscode/tis-plugin
