import logging
import conn
import function as fc
import math
from multiprocessing import Process

# todo: 有部分城市小区列表里链接的是另一个城市的小区，比如廊坊第一页里有个荣盛阿尔卡迪亚永清花语城，链接是北京的。


def get_detail_url(city_code, community_code):
    community_page_path = f'data/city/{city_code}/community/{community_code}.html'
    try:
        page = fc.read_page(community_page_path, "GBK")
    except Exception as e:
        try:
            page = fc.read_page(community_page_path, "GB2312")
        except Exception as e:
            logging.warning(f"读取小区页面编码异常,message={e}")
            return False

    detail_url = page('#xqwxqy_C01_17')('div>span>a').attr('href')
    detail_type = 1
    if detail_url is None:
        # 这是另外一种页面形式
        detail_url = page.find("div[class='floatr']")('a').attr('href')
        detail_type = 2
        if detail_url is None:
            if page('#esf_fangyuanlist')('div>span>a').text() == "查看全部房源":
                detail_type = 3
            elif page('#fangjiazs')('div').eq(0).text().find("价格走势") != -1:
                detail_type = 4
            else:
                detail_url = page('#xfptxq_B04_14')('p>a').attr('href')
                detail_type = 5
                if detail_url is None:
                    detail_type = 9
                    logging.warning(f"检测到新的小区页面,小区ID={community_code}")

    conn.mysql(f"update inf_community set detail_type = {detail_type} where community_code = '{community_code}'")
    if detail_type in (1, 2, 5):
        return detail_url
    elif detail_type in (3, 4):
        return "no_detail"
    else:
        return False


def do_fetch_community(url, city_code, community_code):
    community_page_path = f'data/city/{city_code}/community/{community_code}.html'
    if not fc.fetch_page(url, community_page_path):
        return False
    detail_url = get_detail_url(city_code, community_code)
    if detail_url is False:
        return False
    if detail_url == "no_detail":
        return True
    detail_page_path = f'data/city/{city_code}/detail/{community_code}.html'
    if not fc.fetch_page(detail_url, detail_page_path):
        return False
    return True


def vprocess(process_num, all_community):
    conn.link()
    for Community in all_community:
        if Community == 0:
            continue
        if not do_fetch_community(Community[0], Community[1], Community[2]):
            logging.warning(f"抓取小区详细页面失败.小区ID={Community[2]}")
            continue
        sql = f"update inf_community set status = 1 where id={Community[3]} and website='房天下'"
        conn.mysql(sql)
    conn.close()
    logging.info(f"进程 {process_num} 结束.")


def do_fetch():
    conn.link()
    all_community = conn.get_all("select url,city_code,community_code,id from inf_community"
                                 " where status = 0 and website='房天下'")
    conn.close()
    if all_community == ():
        return False

    count_city = len(all_community)

    process_part = math.ceil(count_city / fc.process_max_num)
    process_num = fc.process_max_num
    if count_city < process_num:
        process_num = count_city

    part_community = [[0 for col in range(process_part)] for row in range(process_num)]
    for i, a_community in enumerate(all_community):
        fpart = int(i / process_num)
        spart = i - fpart * process_num
        part_community[spart][fpart] = a_community

    for _process_num in range(process_num):
        logging.info(f"启动进程 {_process_num}")
        p = Process(target=vprocess, args=(_process_num, part_community[_process_num],))
        p.start()


if __name__ == '__main__':
    do_fetch()

