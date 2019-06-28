# ! /bin/sh
# ------------在这里设置要上传的文件路径---------------------
filepath=/root/test.dat

# ---------------------upload test-----------------------
source ytfs.ev

if [ -z $YTFS_HOME ]; then  
    echo "Environment variable 'YTFS_HOME' not found "
    exit 0;
fi 

echo "YTFS_HOME:$YTFS_HOME"
cd $YTFS_HOME

while  IFS='=' read var val
do
    if [[ $var == 'wrapper.java.command' ]]
    then
         java_cmd=${val:0:${#val}-1}
    elif [[ $var == 'wrapper.java.additional.1' ]]
    then
        java_opts="$java_opts ${val:0:${#val}-1}"
    elif [[ $var == 'wrapper.java.additional.2' ]]
    then
        java_opts="$java_opts ${val:0:${#val}-1}"
    elif [[ $var == 'wrapper.java.additional.3' ]]
    then
        java_opts="$java_opts ${val:0:${#val}-1}"
    elif [[ $var == 'wrapper.java.classpath.1' ]]
    then
        classpath=${val:0:${#val}-1}
    fi 
done < ytfs.conf
 
mainclass="com.ytfs.client.examples.SDKTest"

cmd="$java_cmd $java_opts -classpath $classpath $mainclass $filepath"
echo "cmd: $cmd"
$cmd