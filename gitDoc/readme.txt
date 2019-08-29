20190825

#git 

##[my learning]
git config --global user.name ""
git config --global user.email ""

SSH
ssh-keygen -t rsa -C xxx@xx.com

git - settings - ssk and GPG KEYS
NEW SSH - id_rsa.pub 加到Server上

测试
ssh -T git@github.com
.ssh中会多出一个known_hosts文件

本地创建一个项目，远程创建一个，将二者关联起来
local: create repos - push to remote 
mkdir abc
cd abc
git init -- this is a git project.

remote:
new project - https://github.com/xxxx.git

关联:

git remote add origin xxxx@xxx.git


第一次发布项目（提交）
git add .
git commit -m ""
git push -u origin master


第一次clone项目
git clone git@xxx.git


开发过程中： 
提交:local->remote
git add . 
git commit -m "to branch"
git push origin master


更新：remote->local
git pull


##[my practice]

1. 配置免密钥登录
2. git clone https://github.com/juhao666/plsql.git  #输入了一次密码
3. cd plsql
4. git checkout -b develop #创建Develop分支。 
5. git branch #查看当前分支
6. git add . #到暂缓区
7. git commit -m "initial codes from FM" #提及到本地分支
8. git push origin develop #成功将本地分支及分支内的修改push到远程develop（远程之前是没有Develop分支的）
