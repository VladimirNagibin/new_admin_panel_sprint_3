#!/bin/bash

python manage.py makemigrations --no-input
python manage.py migrate --no-input
python manage.py collectstatic --no-input
cp -r /opt/app/static/. /var/www/static/

set -e

chown www-data:www-data /var/log

uwsgi --strict --ini uwsgi.ini
