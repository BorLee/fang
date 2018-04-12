import logging
import conn
import function as fc


def do_fetch_housing(url, city_code, city_name):
    # 获取小区页面做检测
    if not fc.fetch_page(url, 'data/temp/temp.html'):
        return False
    page = fc.read_page("data/temp/temp.html", "GBK")

    page_title = page('title').text()
    if page_title.find(city_name) == -1:
        logging.warning(f"city_name:{city_name},city_code:{city_code},webtitle != city_name")
        conn.mysql(f"update inf_city set status=8 where city_code='{city_code}'")
        return False

    # 获取小区页面最大分页值，如果没有分页则说明没有收录小区信息
    page_split = page('#houselist_B14_01')
    if page_split.html() is None or page_split.html() == 'None':
        logging.warning(f'没有找到小区信息: {url}')
        conn.mysql(f"update inf_city set status = 9 where city_code = '{city_code}' and website='房天下'")
        return False
    page_split = page_split('.txt').text()
    page_split_max = int(page_split.strip('共页'))
    # 抓取小区各个分页
    for num in range(1, page_split_max + 1):
        split_url = url + '__0_0_0_0_' + str(num) + '_0_0_0/'
        file_path = f'data/city/{city_code}/index/{city_code}_{str(num)}.html'
        if not fc.fetch_page(split_url, file_path):
            return False
    conn.mysql(f"update inf_city set status = 1 where city_code = '{city_code}' and website='房天下'")
    return True


def do_fetch():
    all_city_code = conn.get_all("select city_code,city_name from inf_city where status = 0 and website='房天下'")
    if all_city_code == ():
        return False
    for City_code in all_city_code:
        city_code = City_code[0]
        city_name = City_code[1]
        if city_code == 'bj':
            url = 'http://esf.fang.com/housing/'
            # 北京的小区信息不在bj域名下
        else:
            url = 'http://esf.' + city_code + '.fang.com/housing/'
        do_fetch_housing(url, city_code, city_name)
    return True


if __name__ == '__main__':
    conn.link()
    for x in range(1, 4):
        if not do_fetch():
            break
    logging.info(f"community_list 抓取完毕,循环抓取了{x}次.")
    conn.close()
