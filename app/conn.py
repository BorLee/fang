import sys
import json
from pathlib import Path
import logging
import pymysql
import pymysql.cursors

db = ""


def load_config():
    config_name = 'config.json'
    logging.info('使用配置文件 "{}".'.format(config_name))
    config_file = Path(__file__).parent.joinpath('../conf/', config_name)
    if not config_file.exists():
        config_name = 'config.default.json'
        logging.warning('配置文件不存在, 使用默认配置文件 "{}".'.format(config_name))
        config_file = config_file.parent.joinpath(config_name)
    try:
        config_file = config_file.resolve()
        config_dict = json.loads(config_file.read_text())
    except Exception as e:
        sys.exit('错误: 配置文件载入失败: {}'.format(e))
    return config_dict


def link():
    global db
    config = load_config()

    config = config['mysql_inf']
    host = config['host']
    user = config['user']
    password = config['password']
    db = config['db']
    port = config['port']
    charset = config['charset']

    try:
        db = pymysql.connect(host=host, user=user, password=password, db=db, port=port, charset=charset)
    except Exception as e:
        sys.exit('错误: 数据库连接失败: {}'.format(e))
        return False

    return logging.info('连接数据库成功！')


def close():
    global db

    if db is None or db == "":
        logging.error('数据库未连接！')
        return False

    db.close()
    return logging.info('数据库连接关闭.')


def mysql(sql):
    if sql is None or sql == "":
        logging.error('SQL语句为空.')
        return False
    global db
    if db is None or db == "":
        logging.error('数据库未连接！')
        return False
    cursor = db.cursor()
    try:
        cursor.execute(sql)
        db.commit()
    except Exception as e:
        logging.info(f'# 执行SQL {sql}')
        logging.error(f'SQL执行失败, message="{e}"')
        db.rollback()
        return False
    return True


def get_all(sql):
    if sql is None or sql == "":
        logging.error('SQL语句为空.')
        return False
    global db
    if db is None or db == "":
        logging.error('数据库未连接！')
        return False
    cursor = db.cursor()
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
    except Exception as e:
        logging.error(f'SQL查询失败, message="{e}"')
        return False
    return results


def get_one(sql):
    if sql is None or sql == "":
        logging.error('SQL语句为空.')
        return False
    global db
    if db is None or db == "":
        logging.error('数据库未连接！')
        return False
    cursor = db.cursor()
    try:
        cursor.execute(sql)
        results = cursor.fetchone()
    except Exception as e:
        logging.error(f'SQL查询失败, message="{e}"')
        return False
    return results
