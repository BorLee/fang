import logging
import requests
import re
import os
from pathlib import Path
from pyquery import PyQuery as pq
import captcha.tensorflow_cnn as tesorflow

base_path = Path(__file__).parent.parent
process_max_num = 1
log_path = base_path.joinpath('log/log.log')
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                  " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"}

log_format = '%(asctime)s %(name)s[%(module)s] %(levelname)s: %(message)s'
logging.basicConfig(filename=log_path, format=log_format, level=logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter(log_format)
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


def do_verified(v_url):
    logging.info("尝试进行机器验证...")
    img_dir = base_path.joinpath('app/captcha/captcha-image.jpg')

    try:
        img = requests.get("http://search.fang.com/captcha-verify/captcha-image", headers=headers, timeout=30)
        logging.info("抓取到验证码.")
    except Exception as e:
        logging.error(f'抓取验证码失败: message={e}')
        return False
    f = open(img_dir, 'wb')
    f.write(img.content)
    f.close()
    logging.info(f"保存验证码到{img_dir}")
    # code = input('请输入验证码：')
    code = tesorflow.get_vcode()
    logging.info(f"尝试验证码: {code}")
    postdata = {
        'code': str(code)
    }
    checkUrl = v_url
    requests.post(checkUrl, headers=headers, cookies=requests.utils.dict_from_cookiejar(img.cookies),
                  data=postdata, timeout=30)
    return False


def set_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return True


def get_all_files(path):
    return os.listdir(base_path.joinpath(path))


def remove_html(context, space=0):
    if context is None or context == "":
        return None
    context = re.sub(r'</?\w+[^>]*>', '', context)
    if space == 0:
        context = context.replace(' ', '')
    context = context.replace('\n', '')
    return context


def read_page(page_path, encoding):
    file = base_path.joinpath(page_path)
    page = pq(file.read_text(encoding=encoding))
    return page


def fetch_page(url, page_path):
    logging.info(f'抓取URL={url}')

    try:
        r = requests.get(url, headers=headers, timeout=30)
        v_url = r.url
        r.raise_for_status()
        save_file = base_path.joinpath(page_path)
        set_dir(save_file.parent)
        save_file.write_bytes(r.content)
    except Exception as e:
        logging.error(f'抓取失败: message={e}')
        return False
    try:
        # 比较特殊的是标准页面是GBK编码的，验证码页面是UTF8编码的
        page = read_page(page_path, 'utf-8')
        page_title = page('title').text()
        if page_title.index("验证") != -1:
            logging.warning("保存的文件检测到验证码!")
            os.remove(base_path.joinpath(page_path))
            do_verified(v_url)
            logging.info("重新读取页面.")
            fetch_page(url, page_path)
    finally:
        return True


def get_city_code(context):
    end = context.find('.fang')
    if end == -1:
        return False
    start = context.find('/') + 2
    city_code = context[start:end]
    if city_code == 'world':
        return False
    city_code = city_code.replace('esf.', '')
    return city_code
