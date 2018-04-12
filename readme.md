### file
+ fetch_city:抓取城市信息存到inf_city表
+ fetch_community_list:抓取小区列表页面到本地
+ parser_community_list:解析小区列表页面，并提交小区信息到inf_community
+ fetch_community:抓取小区页面和详情页面
+ parser_community:解析小区页面和详情页面，并更新数据到inf_community
+ conf/config.json:配置mysql连接

### how to use
#### my environment：wsl ubuntu + python3.6
```
git clone https://github.com/BorLee/fang
cd fang
sudo apt-get install p7zip-full
7z x app/captcha/captcha.7z -oapp/captcha
```
#### install tkinter module
python3 
```
sudo apt-get install python3-tk
```
python36
```
sudo apt-get install python3.6-tk
```
if error about Couldn't find any package by glob 'python3.6-dev', add the package repository
```
sudo add-apt-repository ppa:fkrull/deadsnakes
sudo apt-get update
```
#### requirements
```
sudo python3.6 -m pip install -r requirements.txt
```
#### mysql config
```
mv conf/config.default.json conf/config.json
vim conf/config.json
```
#### create a log folder
```
mkdir log
```
#### start
```
python3.6 app/fetch_city.py && python3.6 app/fetch_community_list.py && python3.6 app/parser_community_list.py && python3.6 app/fetch_community.py
```
#### show status
```
tail -f log/log.log
```