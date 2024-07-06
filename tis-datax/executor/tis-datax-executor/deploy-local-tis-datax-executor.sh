mvn clean install -Dappname=all -Dmaven.test.skip=true
mkdir -p /opt/tis/tis-datax-executor
rm -rf   /opt/tis/tis-datax-executor/*
tar xvf ../tis-datax-executor.tar.gz  -C /opt/tis/
rm -rf ./target
