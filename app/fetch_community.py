import logging
import conn
import function as fc


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
    if detail_url is None:
        # 这是另外一种页面形式
        detail_url = page.find("div[class='floatr']")('a').attr('href')
        if detail_url is None:
            if page('#esf_fangyuanlist')('div>span>a').text() == "查看全部房源":
                return "no_detail"
            else:
                logging.warning(f"检测到新的小区页面,小区ID={community_code}")
                return False
    return detail_url


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


def do_fetch():
    all_community = conn.get_all("select url,city_code,community_code,id from inf_community"
                                 " where status = 0 and website='房天下'")
    if all_community == ():
        return False
    for Community in all_community:
        if not do_fetch_community(Community[0], Community[1], Community[2]):
            logging.warning(f"抓取小区详细页面失败.小区ID={Community[2]}")
            continue
        sql = f"update inf_community set status = 1 where id={Community[3]} and website='房天下'"
        conn.mysql(sql)


if __name__ == '__main__':
    conn.link()
    for x in range(1, 4):
        if not do_fetch():
            break
    logging.info("community 抓取完毕.")
    conn.close()
