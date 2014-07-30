Zookeeper管理界面
====================
通过可视化界面对Zookeeper进行CRUD操作。

> java7运行环境

安装
====================
1. mvn clean install
2. 将最终的jar文件和config.cfg放在一起，并在config.cfg中修改zookeeper运行节点，leader节点放在前面，如：server1:2181,server2:2181。
3. 运行：nohup java -jar zkui-2.0-SNAPSHOT-jar-with-dependencies.jar & 
4. 界面地址<a href="http://localhost:9090">http://localhost:9090</a> 

登陆信息
====================
用户名: admin, 密码: manager (管理员权限，支持CRUD操作)

用户名: appconfig, 密码: appconfig (只读权限, 支持只读操作)

配置config.cfg

相关技术点
====================
1. 嵌入式Jetty服务
2. Freemarker模板
3. H2 DB
4. JDBC接口
5. JSON
6. SLF4J日志管理
7. Zookeeper分布式协同管理
8. Apache Commons文件上传
9. Bootstrap
10. Jquery
11. Flyway DB migration

本项目优点
====================
1. CRUD operation on zookeeper properties.
2. Export properties.
3. Import properties via call back url.
4. Import properties via file upload.
5. History of changes + Path specific history of changes.
6. Search feature.
7. Rest API for accessing Zookeeper properties.
8. Basic Role based authentication.
9. LDAP authentication supported.
10. Root node /zookeeper hidden for safety.

导入文件格式
====================
#add property
/appconfig/path=property=value
#remove a propery
-/path/property

You can either upload a file or specify a http url of the version control system that way all your zookeeper changes will be in version control. 

导出文件格式
====================
/appconfig/path=property=value

You can export a file and then use the same format to import.

SOPA/PIPA BLACKLISTED VALUE
====================
All password will be displayed as SOPA/PIPA BLACKLISTED VALUE for a normal user. Admins will be able to view and edit the actual value upon login.
Password will be not shown on search / export / view for normal user.
For a property to be eligible for black listing it should have (PWD / pwd / PASSWORD / password) in the property name.

LDAP
====================
If you want to use LDAP authentication provide the ldap url. This will take precedence over roleSet property file authentication.
ldapUrl=ldap://<ldap_host>:<ldap_port>/dc=mycom,dc=com
If you don't provide this then default roleSet file authentication will be used.

REST风格
====================
A lot of times you require your shell scripts to be able to read properties from zookeeper. This can now be achieved with a http call. Password are not exposed via rest api for security reasons. The rest call is a read only operation requiring no authentication.

Eg:
http://localhost:9090/acd/appconfig?propNames=foo&host=myhost.com
This will first lookup the host name under /appconfig/hosts and then find out which path the host point to. Then it will look for the property under that path.

There are 2 additional properties that can be added to give better control.
cluster=cluster1
http://localhost:9090/acd/appconfig?propNames=foo&cluster=cluster1&host=myhost.com
In this case the lookup will happen on lookup path + cluster1.

app=myapp
http://localhost:9090/acd/appconfig?propNames=foo&app=myapp&host=myhost.com
In this case the lookup will happen on lookup path + myapp.

A shell script will call this via
MY_PROPERTY="$(curl -f -s -S -k "http://localhost:9090/acd/appconfig?propNames=foo&host=`hostname -f`" | cut -d '=' -f 2)"
echo $MY_PROPERTY

局限性
====================
1. ACLs are not yet fully supported

快照
====================
Basic Role Based Authentication
<br/>
<img src="images/zkui-0.png"/>
<br/>

Dashboard Console
<br/>
<img src="images/zkui-1.png"/>
<br/>

CRUD Operations
<br/>
<img src="/images/zkui-2.png"/>
<br/>

Import Feature
<br/>
<img src="images/zkui-3.png"/>
<br/>

Track History of changes
<br/>
<img src="images/zkui-4.png"/>
<br/>

Status of Zookeeper Servers
<br/>
<img src="images/zkui-5.png"/>
<br/>

