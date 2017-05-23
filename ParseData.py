
# coding: utf-8

from ConnectionPool import MySQLConnector as pool
import json

class ParseData:
    @staticmethod
    def parseData(item, pre_indent = ''):
        indent = pre_indent
        indent_2 = indent + '  '
        indent_4 = indent_2 + '  '
        indent_6 = indent_4 + '  '
        indent_8 = indent_6 + '  '
        indent_10 = indent_8 + '  '

        cnn = pool.getConnection()
        
        sid = item['id']
        print indent + 'Starting parse data(id = {})...'.format(sid)
        
        # test if status already exists
        print ''
        print indent + 'Checking if status(id = {}) already exists...'.format(sid)
        select_sql = 'select id from status where id = {}'.format(sid)
        try:
            cursor = cnn.cursor(buffered = True)
            cursor.execute(select_sql)
            result = cursor.fetchall()
            cnn.commit()
        except mysql.connector.Error as err:
            print indent_2 + 'Somthing went wrong in checking status record: {}'.format(err)
            cnn.rollback()
            cnn.close()
            raise
        if len(result) != 0:
            print indent_2 + 'Status(id = {}) already exists!'.format(sid)
        else:
            print indent_2 + 'No such status(id = {}) there!'.format(sid)
            print indent_2 + 'Attempting to adding status(id = {})...'.format(sid)
            lang = item['lang']
            sid_str = item['id_str']
            retweet_count = item['retweet_count'] if 'retweet_count' in item.keys() else ''
            text = item['text'] if 'text' in item.keys() else ''
            source = item['source'] if 'source' in item.keys() else ''
            created_at = item['created_at'] if 'created_at' in item.keys() else ''
            favorite_count = item['favorite_count'] if 'favorite_count' in item.keys() else ''

            media = item['media'] if 'media' in item.keys() else None
            hashtags = item['hashtags'] if 'hashtags' in item.keys() else None
            user = item['user'] if 'user' in item.keys() else None
            user_mentions = item['user_mentions'] if 'user_mentions' in item.keys() else None
            retweeted_status = item['retweeted_status'] if 'retweeted_status' in item.keys() else None
            description = item['description'] if 'description' in item.keys() else None

            # for media
            m_flag = False
            print ''
            print indent + '1. Try to add media...'
            if media != None and len(media) != 0:
                m_flag = True
                media_id = []
                for it in media:
                    mid = it['id']
                    print indent_2 + 'Attempting to add media(id = {})...'.format(mid)
                    # test media if already exists
                    print indent_4 + 'Checking if media(id = {}) already exists...'.format(mid)
                    select_sql = 'select id from infx547.media where id = {}'.format(mid)
                    try:
                        cursor = cnn.cursor(buffered = True)
                        cursor.execute(select_sql)
                        result = cursor.fetchall()
                        cnn.commit()
                    except mysql.connector.Error as err:
                        print indent_6 + 'Somthing went wrong in checking media(id = {}): {}'.format(mid, err)
                        cnn.rollback()
                        cnn.close()
                        raise

                    if len(result) != 0:
                        print indent_6 + 'Media(id = {}) already exists!'.format(mid)
                    else: 
                        print indent_6 + 'No such media (id = {}) there!'.format(mid)
                        print indent_4 + 'Attemping to add media(id = {})...'.format(mid)

                        expanded_url = it['expanded_url']
                        display_url = it['display_url']
                        url = it['url']
                        media_url_https = it['media_url_https']
                        mtype = it['type']
                        media_url = it['media_url']

                        sizes = it['sizes']
                        large_h = sizes['large']['h']
                        large_w = sizes['large']['w']
                        large_resize = sizes['large']['resize']

                        small_h = sizes['small']['h']
                        small_w = sizes['small']['w']
                        small_resize = sizes['small']['resize']

                        thumb_h = sizes['thumb']['h']
                        thumb_w = sizes['thumb']['w']
                        thumb_resize = sizes['thumb']['resize']

                        # test sizes if already exists
                        print ''
                        print indent_6 + 'Attempting to add size...'
                        print indent_8 + 'Checking if size already exists...'
                        select_sql = '''
                        select id from infx547.sizes where large_h = {} AND large_w = {} AND 
                        large_resize = '{}' AND small_h = {} AND small_w = {} AND small_resize = '{}' 
                        AND thumb_h = {} AND thumb_w = {} AND thumb_resize = '{}' 
                        '''.format(large_h, large_w, large_resize, small_h, small_w, 
                                   small_resize, thumb_h, thumb_w, thumb_resize)
                        try:
                            cursor = cnn.cursor(buffered = True)
                            cursor.execute(select_sql)
                            result = cursor.fetchall()
                            cnn.commit()
                        except mysql.connector.Error as err:
                            print indent_10 + 'Somthing went wrong in checking sizes: {}'.format(err)
                            cnn.rollback()
                            cnn.close()
                            raise

                        if len(result) != 0:
                            size_id = result[0][0]
                            print indent_10 + 'Size(id = {}) already exists!'.format(size_id)
                            print ''
                        else:
                            print indent_10 + "No such sizes there!"
                            print indent_8 + 'Adding the size...'
                            # insert into sizes
                            insert_sql = '''insert into sizes(large_h, large_w, large_resize, small_h, small_w, small_resize,
                            thumb_h, thumb_w, thumb_resize ) value(%s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                            try:
                                cursor = cnn.cursor()
                                cursor.execute(insert_sql, (large_h, large_w, large_resize, small_h, small_w, small_resize, 
                                               thumb_h, thumb_w, thumb_resize))
                                cnn.commit()
                                size_id = cursor.lastrowid

                            except mysql.connector.Error as err:
                                print indent_10 + 'Somthing went wrong in inserting sizes: {}'.format(err)
                                cnn.rollback()
                                cnn.close()
                                raise
                            print indent_10 + 'Successfully adding sizes(id = {})!'.format(size_id)

                        # insert into media
                        print indent_6 + 'Adding media(id = {})...'.format(mid)
                        insert_sql = '''insert into media(id, expanded_url, display_url, url, media_url_https, 
                        type, media_url, sizes) value(%s, %s, %s, %s, %s, %s, %s, %s)'''
                        try:
                            cursor = cnn.cursor()
                            cursor.execute(insert_sql, (mid, expanded_url, display_url, url, media_url_https, 
                                                        mtype, media_url, size_id))
                            cnn.commit()
                        except mysql.connector.Error as err:
                            print indent_8 + 'Somthing went wrong in inserting media(id = {}): {}'.format(mid, err)
                            cnn.rollback()
                            cnn.close()
                            raise
                        print indent_8 + 'Successfully adding media(id = {})!'.format(mid)

                    # get media id
                    media_id.append(mid)

            # for hashtags
            h_flag = False
            print ''
            print indent + '2. Try to adding hashtag: ...'
            if hashtags != None and len(hashtags) != 0:
                h_flag = True
                hts = []
                for it in hashtags:
                    text = it['text']
                    print indent_2 + 'Attempting to adding hashtag: "{}" ...'.format(text)
                    # test hashtag if already exists
                    print indent_4 + 'Checking if hashtag "{}" already exists ...'.format(text)
                    select_sql = 'select id from hashtag where text = "{}"'.format(text)
                    try:
                        cursor = cnn.cursor(buffered = True)
                        cursor.execute(select_sql)
                        result = cursor.fetchall()
                        cnn.commit()
                    except mysql.connector.Error as err:
                        print indent_6 + 'Somthing went wrong in querying hashtag(text="{}": {}'.format(text, err)
                        cnn.rollback()
                        cnn.close()
                        raise

                    if len(result) != 0:
                        print indent_6 + 'Hashtag "{}" already exists!'.format(text)
                        hts.append(result[0][0])
                    else:
                        print indent_6 + 'No hashtag named "{}" there!'.format(text)
                        print indent_4 + 'Adding Hashtag: "{}..."'.format(text)

                        # insert into hashtag
                        insert_sql = 'insert into hashtag(text) value("{}")'.format(text)
                        try:
                            cursor = cnn.cursor()
                            cursor.execute(insert_sql)
                            cnn.commit()
                            hts.append(cursor.lastrowid)
                        except mysql.connector.Error as err:
                            print indent_6 + 'Somthing went wrong in adding hashtag(text = "{}"): {}'.format(text, err)
                            cnn.rollback()
                            cnn.close()
                            raise
                        print indent_6 + 'Successfully adding Hashtag "{}"!'.format(text)
                        
            # for user
            uid = ''
            print ''
            print indent + '3. Try to add user...'
            if user != None:
                uid = user['id']
                print indent_2 + 'Attempting to add user(id = {})...'.format(uid)
                # test user if already exists
                print indent_4 + 'Checking if user(id = {}) already exists...'.format(uid)
                select_sql = 'select id from user where id = {} and friends_count is not null'.format(uid)
                try:
                    cursor = cnn.cursor(buffered = True)
                    cursor.execute(select_sql)
                    result = cursor.fetchall()
                    cnn.commit()
                except mysql.connector.Error as err:
                    print indent_6 + 'Somthing went wrong in checking user(id = {}): {}'.format(uid, err)
                    cnn.rollback()
                    cnn.close()
                    raise

                if len(result) != 0:
                    print indent_6 + 'User(id = {}) already exists!'.format(uid)
                else:
                    print indent_6 + 'No such user(id = {}) there!'.format(uid)
                    print indent_4 + 'Adding user(id = {})...'.format(uid)
                    
                    ulang = user['lang']
                    utc_offset = user['utc_offset']
                    favourites_count = user['favourites_count']
                    name = user['name']
                    friends_count = user['friends_count']
                    profile_link_color = user['profile_link_color']
                    u_created_at = user['created_at']
                    profile_sidebar_fill_color = user['profile_sidebar_fill_color']
                    time_zone = user['time_zone']
                    profile_image_url = user['profile_image_url']
                    profile_text_color = user['profile_text_color']
                    followers_count = user['followers_count']
                    location = user['location']
                    profile_background_color = user['profile_background_color']
                    statuses_count = user['statuses_count']
                    listed_count = user['listed_count']
                    profile_banner_url = user['profile_banner_url']
                    profile_background_image_url = user['profile_background_image_url']
                    screen_name = user['screen_name']
                    description = user['description'] if 'description' in user.keys() else ''

                    # insert into user
                    insert_sql = '''
                    insert into user(id, lang, utc_offset, favourites_count, name, friends_count, profile_link_color, created_at, 
                    profile_sidebar_fill_color, time_zone, profile_image_url, profile_text_color, followers_count, location, 
                    profile_background_color, statuses_count, listed_count, profile_banner_url, profile_background_image_url, 
                    screen_name, description) value('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'
                    ,'{}','{}','{}','{}','{}','{}')
                    '''.format(uid, ulang, utc_offset, favourites_count, name, friends_count, profile_link_color, u_created_at, 
                    profile_sidebar_fill_color, time_zone, profile_image_url, profile_text_color, followers_count, location, 
                    profile_background_color, statuses_count, listed_count, profile_banner_url, profile_background_image_url, 
                    screen_name, description)

                    try:
                        cursor = cnn.cursor()
                        cursor.execute(insert_sql)
                        cnn.commit()
                    except mysql.connector.Error as err:
                        print indent_6 + 'Somthing went wrong in adding use(id = {}): "{}"'.format(uid, err)
                        cnn.rollback()
                        cnn.close()
                        raise
                    print indent_6 + 'Successfully adding user(id = {})!'.format(uid)


            # for user_mentions
            u_flag = False
            print ''
            print indent + '4. Try to add user_mentions...'
            if user_mentions != None and len(user_mentions) != 0:
                for it in user_mentions:
                    um = []
                    um_id = it['id']
                    print indent_2 + 'Attempting to add user_mentions(uid = {})...'.format(um_id)
                    # test user if already exists:
                    print indent_4 + 'User_mentions: checking if user(id = {}) already exists...'.format(um_id)
                    select_sql = 'select id from user where id = {}'.format(um_id)
                    try:
                        cursor = cnn.cursor(buffered = True)
                        cursor.execute(select_sql)
                        result = cursor.fetchall()
                        cnn.commit()
                    except mysql.connector.Error as err:
                        print indent_6 + 'User_mentions: somthing went wrong in checking user(id = {}): {}'.format(um_id, err)
                        cnn.rollback()
                        cnn.close()
                        raise

                    if len(result) != 0:
                        print indent_6 + 'User_mentions: user(id = {}) already exists!'.format(um_id)
                        um.append(um_id)
                    else:
                        print indent_6 + 'User_mentions: no such user(id = {}) there!'.format(um_id)
                        print indent_4 + 'User_mentions: adding user(id = {})...'.format(um_id)

                        um_screen_name = it['screen_name']
                        um_name = it['name']

                        # insert into user
                        insert_sql = 'insert into user(id, screen_name, name) value("{}", "{}", "{}")'.format(
                            um_id, um_screen_name, um_name)

                        try:
                            cursor = cnn.cursor()
                            cursor.execute(insert_sql)
                            cnn.commit()
                        except mysql.connector.Error as err:
                            print indent_6 + 'User_mentions: somthing went wrong in adding use(id = {}): "{}"'.format(um_id, err)
                            cnn.rollback()
                            cnn.close()
                            raise
                        um.append(um_id)
                        print indent_6 + 'User_mentions: adding user(id = {}) successfully!'.format(um_id)    
            print indent + 'Finished adding user_mentions!'

            # for retweeted_status
            rs_id = None
            print ''
            print indent + '5. Try to add retweeted_status...'
            if retweeted_status != None:
                rs_id = retweeted_status['id']
                print indent_2 + 'Attempting to insert retweeted_status(id = {})...'.format(rs_id)
                ParseData.parseData(retweeted_status, indent_2)
            print indent + 'Finished adding retweeted_status!'

            # insert into status
            print ''
            print indent + '6. Adding status(id = {})...'.format(sid)
            if rs_id == None:
                insert_sql = '''
                insert into status(id, created_at, favorite_count, id_str, 
                lang, retweet_count, source, text, user, description) 
                value('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')
                '''.format(sid, created_at, favorite_count, sid_str, 
                lang, retweet_count, source, text.encode('utf-8'), uid, description)
            else:
                insert_sql = '''
                insert into status(id, created_at, favorite_count, id_str, 
                lang, retweet_count, source, text, user, description, retweeted_status) 
                value('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}', '{}')
                '''.format(sid, created_at, favorite_count, sid_str, 
                lang, retweet_count, source, text.encode('utf-8'), uid, description, rs_id)

            try:
                cursor = cnn.cursor()
                cursor.execute(insert_sql)
                cnn.commit()
            except mysql.connector.Error as err:
                print indent_2 + 'Somthing went wrong in adding status(id = {}): "{}"'.format(sid, err)
                cnn.rollback()
                cnn.close()
                raise
            print indent_2 + 'Successfully adding status(id = {})!'.format(sid)

            # insert into media_status
            print ''
            print indent + 'Media_status:'
            if m_flag:
                print indent + 'Starting adding media_status'
                for mid in media_id:
                    print indent_2 + 'Attempting to add media(id = {}) and status(sid = {})...'.format(mid, sid)
                    print indent_4 + 'Checking if media_status already exists...'
                    # check if media_status already exists
                    select_sql = 'select id from media_status where status = {} and media = {}'.format(sid, mid)
                    try:
                        cursor = cnn.cursor(buffered = True)
                        cursor.execute(select_sql)
                        result = cursor.fetchall()
                        cnn.commit()
                    except mysql.connector.Error as err:
                        print indent_6 + 'Somthing went wrong in checking status_media(sid = {}, mid = {}): "{}"'.format(sid, mid, err)
                        cnn.rollback()
                        cnn.close()
                        raise
                    if len(result) != 0:
                        print indent_6 + 'Media_status(mid = {}, sid = {}) already exists!'.format(mid, sid)
                    else:
                        print indent_6 + 'No such media_status(mid = {}, sid = {}) there!'.format(mid, sid)
                        print indent_4 + 'Adding media(id = {}) and status(id = {}) pair...'.format(mid, sid)
                        insert_sql = 'insert into media_status(media, status) value({}, {})'.format(mid, sid)
                        try:
                            cursor = cnn.cursor()
                            cursor.execute(insert_sql)
                            cnn.commit()
                        except mysql.connector.Error as err:
                            print indent_6 + 'Somthing went wrong in adding media_status(mid = {}, sid = {}): "{}"'.format(mid, sid, err)
                            cnn.rollback()
                            cnn.close()
                            raise
                        print indent_6 + 'Adding media_status(mid = {}, sid = {}) successfully'.format(mid, sid)
                print indent_6 + 'Successfully adding all media_status!'
            else:
                print indent + 'No media_status!'

            # insert into user_mention
            print ''
            print indent + 'User_mention:'
            if u_flag:
                print indent + 'Starting adding user_mention'
                for uid in um:
                    print indent_2 + 'Attempting to add user(id = {}) and status(sid = {})...'.format(uid, sid)
                    print indent_4 + 'Checking if user_mention already exists...'
                    # check if media_status already exists
                    select_sql = 'select id from user_mention where user = {} and status = {}'.format(uid, sid)
                    try:
                        cursor = cnn.cursor(buffered = True)
                        cursor.execute(select_sql)
                        result = cursor.fetchall()
                        cnn.commit()
                    except mysql.connector.Error as err:
                        print indent_6 + 'Somthing went wrong in checking user_mention(uid = {}, sid = {}): "{}"'.format(uid, sid, err)
                        cnn.rollback()
                        cnn.close()
                        raise
                    if len(result) != 0:
                        print indent_6 + 'User_mention(uid = {}, sid = {}) already exists!'.format(uid, sid)
                    else:
                        print indent_6 + 'No such user_mention(uid = {}, sid = {}) there!'.format(uid, sid)
                        print indent_4 + 'Adding user(id = {}) and status(id = {}) pair...'.format(uid, sid)
                        insert_sql = 'insert into user_mention(user, status) value({}, {})'.format(uid, sid)
                        try:
                            cursor = cnn.cursor()
                            cursor.execute(insert_sql)
                            cnn.commit()
                        except mysql.connector.Error as err:
                            print indent_6 + 'Somthing went wrong in adding user_mention(uid = {}, sid = {}): "{}"'.format(uid, sid, err)
                            cnn.rollback()
                            cnn.close()
                            raise
                        print indent_6 + 'Adding user_mention(uid = {}, sid = {}) successfully'.format(uid, sid)
                print indent + 'Successfully adding all user_mention!'
            else:
                print indent + "No user_mention!"

            # insert into hashtag_status
            print ''
            print indent + 'Hashtag_status:'
            if h_flag:
                print indent + 'Starting adding hashtag_status'
                for hid in hts:
                    print indent_2 + 'Attempting to add hashtag(id = {}) and status(id = {})...'.format(hid, sid)
                    print indent_4 + 'Checking if hashtag_status already exists...'
                    # check if media_status already exists
                    select_sql = 'select id from hashtag_status where hashtag = {} and status = {}'.format(hid, sid)
                    try:
                        cursor = cnn.cursor(buffered = True)
                        cursor.execute(select_sql)
                        result = cursor.fetchall()
                        cnn.commit()
                    except mysql.connector.Error as err:
                        print indent_6 + 'Somthing went wrong in checking hashtag_status(hid = {}, sid = {}): "{}"'.format(hid, sid, err)
                        cnn.rollback()
                        raise
                    if len(result) != 0:
                        print indent_6 + 'Hashtag_status(hid = {}, sid = {}) already exists!'.format(hid, sid)
                    else:
                        print indent_6 + 'No such hashtag_status(hid = {}, sid = {}) there!'.format(hid, sid)
                        print indent_4 + 'Adding hashtag(id = {}) and status(id = {}) pair...'.format(hid, sid)
                        insert_sql = 'insert into hashtag_status(hashtag, status) value({}, {})'.format(hid, sid)
                        try:
                            cursor = cnn.cursor()
                            cursor.execute(insert_sql)
                            cnn.commit()
                        except mysql.connector.Error as err:
                            print indent_6 + 'Somthing went wrong in adding hashtag_status(hid = {}, sid = {}): "{}"'.format(hid, sid, err)
                            cnn.rollback()
                            cnn.close()
                            raise
                        print indent_6 + 'Adding hashtag_status(hid = {}, sid = {}) successfully'.format(hid, sid)
                print indent + 'Successfully adding all hashtag_status!'
            else:
                print indent + 'No hashtag_status!'
                
        print 'Finished export data(id = {})'.format(sid)
        cnn.close()



if __name__ == '__main__':
    path = 'preforERD.json'

    with open(path, 'r') as file:
        data = json.load(file)
    # print json.dumps(data['result'], indent=4)
    
    for item in data['result']:
        ParseData.parseData(item, '')



    #print json.dumps(data['result'][0]['user_mentions'], indent=4)

    #print json.dumps(data['result'][0]['retweeted_status'], indent=4)

    #print json.dumps(data['result'][0]['user'], indent=4)

    # for k in data['result'][0]['retweeted_status']:
    #     if k in data['result'][0]:
    #         continue
    #     print k


