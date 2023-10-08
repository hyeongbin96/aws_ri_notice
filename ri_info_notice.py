import db_info
import pymysql
import requests
from datetime import datetime

google_chat_webhook_url = db_info.google_chat_webhook_url

current_date = datetime.now()

db_connection = pymysql.connect(
host = db_info.host,
port = db_info.port,
user = db_info.user,
passwd = db_info.passwd,
db = db_info.db,
cursorclass = pymysql.cursors.DictCursor
)

sql_query = """
SELECT billdbdev.ri.*, billdbdev.cloud_account.account
FROM billdbdev.ri
INNER JOIN cloud_account ON ri.account_id = cloud_account.account_id
WHERE state = 'active'
"""

with db_connection.cursor() as cursor:
    cursor.execute(sql_query)
    result = cursor.fetchall()

    messages = []

    # 계정 이름을 추적하기 위한 변수와 현재 날짜 변수
    previous_account = ""
    current_datetime = datetime.now()

    for row in result:
        account = row['account']
        end_date = datetime.combine(row['end_date'], datetime.min.time())
        delta = end_date - current_datetime
        
        if delta.days < 150:
            if previous_account is None or previous_account != account:
                if previous_account is not None:
                    messages.append("\n")
                    
                messages.append(f"<*{account} ({row['account_id']})*>")
                previous_account = account
                    
            formatted_end_date = end_date.strftime("%Y-%m-%d")
            messages.append(f"{account}의 RI ({row['instance_type']}({row['platform']})) {delta.days}일 후 만료됩니다. (만료 날짜 : {formatted_end_date})")
                    
    for message in messages:
        requests.post(google_chat_webhook_url, json={'text': message})
                        
db_connection.close()