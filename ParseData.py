
# coding: utf-8
import mysql.connector
from ConnectionPool import MySQLConnector as pool
import json

class ParseData:
    @staticmethod
    def parseData(item, progress, pre_indent = ''):
        indent = '[' + progress + ']:' + pre_indent
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
            lang = str(item['lang'].encode('utf-8')) if 'lang' in item.keys() and item['lang'] != None else ''
            sid_str = str(item['id_str'].encode('utf-8')) if 'id_str' in item.keys() and item['id_str'] != None else ''
            retweet_count = str(item['retweet_count']) if 'retweet_count' in item.keys() and item['retweet_count'] != None else ''
            text = str(item['text'].encode('utf-8')) if 'text' in item.keys() and item['text'] != None else ''
            source = str(item['source'].encode('utf-8')) if 'source' in item.keys() and item['source'] != None else ''
            created_at = str(item['created_at'].encode('utf-8')) if 'created_at' in item.keys() and item['created_at'] != None else ''
            favorite_count = str(item['favorite_count']) if 'favorite_count' in item.keys() and item['favorite_count'] != None else ''
            description = str(item['description'].encode('utf-8')) if 'description' in item.keys() and item['description'] != None else ''

            text = text.replace('"', "'")
            source = source.replace('"', "'")
            created_at = created_at.replace('"', "'")
            favorite_count = favorite_count.replace('"', "'")
            description = description.replace('"', "'")

            text = text.replace('\\', "")
            source = source.replace('\\', "")
            created_at = created_at.replace('\\', "")
            favorite_count = favorite_count.replace('\\', "")
            description = description.replace('\\', "")
            description = description.replace('\\', "")

            hashtags = item['hashtags'] if 'hashtags' in item.keys() else None
            user = item['user'] if 'user' in item.keys() else None
            user_mentions = item['user_mentions'] if 'user_mentions' in item.keys() else None
            retweeted_status = item['retweeted_status'] if 'retweeted_status' in item.keys() else None
            

            # for hashtags
            h_flag = False
            print ''
            print indent + '2. Try to adding hashtag: ...'
            if hashtags != None and len(hashtags) != 0:
                h_flag = True
                hts = []
                for it in hashtags:
                    text = it['text'].encode('utf-8')
                    text = text.replace('"', "'")
                    text = text.replace('\\', "")
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
                        continue

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
                            continue
                        print indent_6 + 'Successfully adding Hashtag "{}"!'.format(text)
            else:
                print indent + "No Hashtag There!"
                        
            # for user
            uid = ''
            print ''
            print indent + '3. Try to add user...'
            if user != None:
                uid = user['id']
                ulang = str(user['lang'].encode('utf-8')) if 'lang' in user.keys() and user['lang'] != None else ''
                utc_offset = str(user['utc_offset']) if 'utc_offset' in user.keys() and user['utc_offset'] != None else ''
                favourites_count = str(user['favourites_count']) if 'favourites_count' in user.keys() and user['favourites_count'] != None else ''
                name = str(user['name'].encode('utf-8')) if 'name' in user.keys() and user['name'] != None else ''
                friends_count = str(user['friends_count']) if 'friends_count' in user.keys() and user['friends_count'] != None else ''
                profile_link_color = str(user['profile_link_color'].encode('utf-8')) if 'profile_link_color' in user.keys() and user['profile_link_color'] != None else ''
                u_created_at = str(user['created_at'].encode('utf-8')) if 'created_at' in user.keys() and user['created_at'] != None else ''
                profile_sidebar_fill_color = str(user['profile_sidebar_fill_color'].encode('utf-8')) if 'profile_sidebar_fill_color' in user.keys() and user['profile_sidebar_fill_color'] != None else ''
                time_zone = str(user['time_zone'].encode('utf-8')) if 'time_zone' in user.keys() and user['time_zone'] != None else ''
                profile_image_url = str(user['profile_image_url'].encode('utf-8')) if 'profile_image_url' in user.keys() and user['profile_image_url'] != None else ''
                profile_text_color = str(user['profile_text_color'].encode('utf-8')) if 'profile_text_color' in user.keys() and user['profile_text_color'] != None else ''
                followers_count = str(user['followers_count']) if 'followers_count' in user.keys() and user['followers_count'] != None else ''
                location = str(user['location'].encode('utf-8')) if 'location' in user.keys() and user['location'] != None else ''
                profile_background_color = str(user['profile_background_color'].encode('utf-8')) if 'profile_background_color' in user.keys() and user['profile_background_color'] != None else ''
                statuses_count = str(user['statuses_count']) if 'statuses_count' in user.keys() and user['statuses_count'] != None else ''
                listed_count = str(user['listed_count']) if 'listed_count' in user.keys() and user['listed_count'] != None else ''
                profile_banner_url = str(user['profile_banner_url'].encode('utf-8')) if 'profile_banner_url' in user.keys() and user['profile_banner_url'] != None else  ''
                profile_background_image_url = str(user['profile_background_image_url'].encode('utf-8')) if 'profile_background_image_url' in user.keys() and user['profile_background_image_url'] != None else ''
                screen_name = str(user['screen_name'].encode('utf-8')) if 'screen_name' in user.keys() and user['screen_name'] != None else ''
                udescription = str(user['description'].encode('utf-8')) if 'description' in user.keys() and user['description'] != None else ''

                ulang = ulang.replace('"', "'")
                utc_offset = utc_offset.replace('"', "'")
                favourites_count = favourites_count.replace('"', "'")
                name = name.replace('"', "'")
                friends_count = friends_count.replace('"', "'")
                profile_link_color = profile_link_color.replace('"', "'")
                u_created_at = u_created_at.replace('"', "'")
                profile_sidebar_fill_color = profile_sidebar_fill_color.replace('"', "'")
                time_zone = time_zone.replace('"', "'")
                profile_image_url = profile_image_url.replace('"', "'")
                profile_text_color = profile_text_color.replace('"', "'")
                followers_count = followers_count.replace('"', "'")
                location = location.replace('"', "'")
                profile_background_color = profile_background_color.replace('"', "'")
                statuses_count = statuses_count.replace('"', "'")
                listed_count = listed_count.replace('"', "'")
                profile_banner_url = profile_banner_url.replace('"', "'")
                profile_background_image_url = profile_background_image_url.replace('"', "'")
                screen_name = screen_name.replace('"', "'")
                udescription = udescription.replace('"', "'")

                ulang = ulang.replace('\\', "")
                utc_offset = utc_offset.replace('\\', "")
                favourites_count = favourites_count.replace('\\', "")
                name = name.replace('\\', "")
                friends_count = friends_count.replace('\\', "")
                profile_link_color = profile_link_color.replace('\\', "")
                u_created_at = u_created_at.replace('\\', "")
                profile_sidebar_fill_color = profile_sidebar_fill_color.replace('\\', "")
                time_zone = time_zone.replace('\\', "")
                profile_image_url = profile_image_url.replace('\\', "")
                profile_text_color = profile_text_color.replace('\\', "")
                followers_count = followers_count.replace('\\', "")
                location = location.replace('\\', "")
                profile_background_color = profile_background_color.replace('\\', "")
                statuses_count = statuses_count.replace('\\', "")
                listed_count = listed_count.replace('\\', "")
                profile_banner_url = profile_banner_url.replace('\\', "")
                profile_background_image_url = profile_background_image_url.replace('\\', "")
                screen_name = screen_name.replace('\\', "")
                udescription = udescription.replace('\\', "")
                udescription = udescription.replace('\\', "")

                print indent_2 + 'Attempting to add user(id = {})...'.format(uid)
                # test user if already exists
                print indent_4 + 'Checking if user(id = {}) already exists...'.format(uid)
                select_sql = 'select id from user where id = {}'.format(uid)
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
                    # check if it is necessary to update user data
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
                        print indent_6 + 'User(id = {}) do not need to be updated!'.format(uid)
                    else:
                        print indent_6 + 'User(id = {}) need to be updated!'.format(uid)
                        print indent_4 + 'Updating user(id = {})...'.format(uid)

                        # update user data
                        update_sql = """
                        update user set 
                        lang = "{}", utc_offset = "{}", favourites_count = "{}", name = "{}", friends_count = "{}", profile_link_color = "{}", 
                        created_at = "{}", profile_sidebar_fill_color = "{}", time_zone = "{}", profile_image_url = "{}", profile_text_color = "{}", 
                        followers_count = "{}", location = "{}", profile_background_color = "{}", statuses_count = "{}", listed_count = "{}", 
                        profile_banner_url = "{}", profile_background_image_url = "{}", screen_name = "{}", description = "{}"
                        where id = "{}"
                        """.format(ulang, utc_offset, favourites_count, name, friends_count, profile_link_color, u_created_at, 
                        profile_sidebar_fill_color, time_zone, profile_image_url, profile_text_color, followers_count, location, 
                        profile_background_color, statuses_count, listed_count, profile_banner_url, profile_background_image_url, 
                        screen_name, udescription, uid)
                        try:
                            cursor = cnn.cursor()
                            cursor.execute(update_sql)
                            cnn.commit()
                        except mysql.connector.Error as err:
                            print indent_6 + 'Somthing went wrong in updating user(id = {}): "{}"'.format(uid, err)
                            print ''
                            print update_sql
                            cnn.rollback()
                            cnn.close()
                            
                        print indent_6 + 'Finishing updating user(id = {})!'.format(uid)
                    
                else:
                    print indent_6 + 'No such user(id = {}) there!'.format(uid)
                    print indent_4 + 'Adding user(id = {})...'.format(uid)

                    # insert into user
                    insert_sql = '''
                    insert into user(id, lang, utc_offset, favourites_count, name, friends_count, profile_link_color, created_at, 
                    profile_sidebar_fill_color, time_zone, profile_image_url, profile_text_color, followers_count, location, 
                    profile_background_color, statuses_count, listed_count, profile_banner_url, profile_background_image_url, 
                    screen_name, description) value("{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}",
                    "{}","{}","{}","{}","{}","{}")
                    '''.format(uid, ulang, utc_offset, favourites_count, name, friends_count, profile_link_color, u_created_at, 
                    profile_sidebar_fill_color, time_zone, profile_image_url, profile_text_color, followers_count, location, 
                    profile_background_color, statuses_count, listed_count, profile_banner_url, profile_background_image_url, 
                    screen_name, udescription)
                    try:
                        cursor = cnn.cursor()
                        cursor.execute(insert_sql)
                        cnn.commit()
                    except mysql.connector.Error as err:
                        print indent_6 + 'Somthing went wrong in adding user(id = {}): "{}"'.format(uid, err)
                        print ''
                        print insert_sql
                        cnn.rollback()
                        cnn.close()
                        raise
                    print indent_6 + 'Successfully adding user(id = {})!'.format(uid)
            else:
                print indent + 'No User There!'


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

                        um_screen_name = str(it['screen_name'].encode('utf-8'))
                        um_name = str(it['name'].encode('utf-8'))

                        um_screen_name = um_screen_name.replace('"', "'")
                        um_name = um_name.replace('"', "'")

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
            else:
                print indent + "No User_Mentions There!"

            # for retweeted_status
            rs_id = None
            print ''
            print indent + '5. Try to add retweeted_status...'
            if retweeted_status != None:
                rs_id = retweeted_status['id']
                print indent_2 + 'Attempting to insert retweeted_status(id = {})...'.format(rs_id)
                ParseData.parseData(retweeted_status, progress, indent_2)
                print indent + 'Finished adding retweeted_status!'
            else:
                print 'No Retweeted_Status There!'

            # insert into status
            print ''
            print indent + '6. Adding status(id = {})...'.format(sid)
            text = text.replace('"',"'")
            if rs_id == None:
                insert_sql = '''
                insert into status(id, created_at, favorite_count, id_str, 
                lang, retweet_count, source, text, user, description) 
                value("{}","{}","{}","{}","{}","{}","{}","{}","{}","{}")
                '''.format(sid, created_at, favorite_count, sid_str, 
                lang, retweet_count, source, text, uid, description)
            else:
                insert_sql = '''
                insert into status(id, created_at, favorite_count, id_str, 
                lang, retweet_count, source, text, user, description, retweeted_status) 
                value("{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}")
                '''.format(sid, created_at, favorite_count, sid_str, 
                lang, retweet_count, source, text, uid, description, rs_id)
            try:
                cursor = cnn.cursor()
                cursor.execute(insert_sql)
                cnn.commit()
            except mysql.connector.Error as err:
                print indent_2 + 'Somthing went wrong in adding status(id = {}): "{}"'.format(sid, err)
                print ''
                print insert_sql
                cnn.rollback()
                cnn.close()
                raise
            print indent_2 + 'Successfully adding status(id = {})!'.format(sid)

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


