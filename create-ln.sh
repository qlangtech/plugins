# https://blog.csdn.net/qq_38425719/article/details/102515854

#CURRENT_PATH=`$(pwd)`;
for f in `find $(pwd)  -name '*.tpi' -print`
do
   echo " ln -s $f "
   rm -f /opt/data/tis/libs/plugins/${f##*/}
   ln -s $f /opt/data/tis/libs/plugins/${f##*/}
done ;

