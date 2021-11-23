rm -rf lib
rm lambdas.zip
mkdir lib
/usr/local/opt/pipenv/bin/pipenv lock -r > requirements.txt
/usr/local/opt/python@3.7/bin/pip3 install -r requirements.txt --no-deps -t lib
zip -r lambdas.zip lib
zip -g lambdas.zip queue_firehose_s3_backups.py
zip -g lambdas.zip recover_firehose_s3_backup.py
