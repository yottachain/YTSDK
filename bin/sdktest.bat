@echo off 

rem ---------在这里设置要上传的文件路径------------

set filepath=f:/aa.txt


rem --------------------upload test----------------------

setlocal enabledelayedexpansion
for /f "tokens=1,2,3 delims==" %%i in (ytfs.conf) do (
 if "%%i"=="wrapper.java.command" set java_cmd=%%j
 if "%%i"=="wrapper.java.initmemory" set java_opts=-Xms%%jM
 if "%%i"=="wrapper.java.maxmemory" set java_opts=!java_opts! -Xmx%%jM
 if "%%i"=="wrapper.java.additional.1" set java_opts=!java_opts! %%j=%%k
 if "%%i"=="wrapper.java.additional.2" set java_opts=!java_opts! %%j=%%k
 if "%%i"=="wrapper.java.additional.3" set java_opts=!java_opts! %%j=%%k
 if "%%i"=="wrapper.java.classpath.1" set classpath=%%j
)
set java_opts=!java_opts! -Dfile.encoding=GBK

set mainclass=com.ytfs.client.examples.SDKTest
set java_cmd=%java_cmd:/=\%
set cmd=%java_cmd% %java_opts% -classpath %classpath% %mainclass% %filepath%
echo cmd: %cmd%
echo enter 'x' to shutdown server, 'r' to restart server ...
%cmd%