# Investing (Athens Stock Exchange) Project

## Setting up Virtual Environment
Within the root folder do:
```
virtualenv -p Investing
source Investing/bin/activate
pip install -r requirements.txt
```

## Git on virtual environment
To make sure that all current changes are pulled in from the repository into the VM, each user should download a [Git client](ps://git-scm.com/download/win) and install it on their accounts with the standard installation options (only changing the notepad option to notepad++), and creating a start icon.

For more information about the following commands visit the official [Gitlad website] (https://about.gitlab.com/installation/#ubuntu)

1. Install git: sudo apt-get install git, OR sudo yum install git 
2. sudo apt-get/yum install -y curl openssh-server ca-certificates
3. sudo apt-get/yum install -y postfix
4. curl https://packages.gitlab.com/install/repositories/gitlab/gitlab-ee/script.deb.sh | sudo bash
5. sudo EXTERNAL_URL="http://www.gitlab.com/Kostas885" apt-get install gitlab-ee
6. vim /etc/gitlab/gitlab.rb - and change the external URl to https://www.gitlab.com/Kostas885
7. sudo gitlab-ctl reconfigure - to apply changes

Gitlab push password: **Investing**


