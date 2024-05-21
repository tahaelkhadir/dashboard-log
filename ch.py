import re
import pymysql
from datetime import datetime

connection = pymysql.connect(host='localhost',
                             user='root',
                             password='taha123',
                             database='logApa')

cursor = connection.cursor()

log_pattern = re.compile(r'(?P<ip>[\d\.]+) - - \[(?P<date>.*?)\] "(?P<method>\w+) (?P<url>.*?) HTTP/\d\.\d" (?P<status>\d+) (?P<size>\d+) "(?P<referrer>.*?)" "(?P<user_agent>.*?)"')

MAX_URL_LENGTH = 255
BATCH_SIZE = 1000  

try:
    with open('access_log') as file:
        lines = file.readlines()
        total_lines = len(lines)
        current_batch = []

        for idx, line in enumerate(lines):
            match = log_pattern.match(line)
            if match:
                data = match.groupdict()
                ip = data['ip']
                date_str = data['date']
                url = data['url'][:MAX_URL_LENGTH]  
                status = int(data['status'])
                user_agent = data['user_agent']

                
                is_static = url.endswith(('.jpg', '.png', '.css', '.js', '.gif'))

               
                operating_system = 'Unknown OS'
                browser = 'Unknown Browser'
                if 'Windows' in user_agent:
                    operating_system = 'Windows'
                elif 'Macintosh' in user_agent:
                    operating_system = 'Mac OS'
               

                if 'Firefox' in user_agent:
                    browser = 'Firefox'
                elif 'Chrome' in user_agent:
                    browser = 'Chrome'
               

                
                request_date = datetime.strptime(date_str.split()[0], '%d/%b/%Y:%H:%M:%S')

                current_batch.append((ip, url, is_static, status, request_date, operating_system, browser))

                
                if len(current_batch) >= BATCH_SIZE or idx == total_lines - 1:
                    cursor.executemany('INSERT INTO visitors (ip) VALUES (%s) ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)', [(ip,) for ip, _, _, _, _, _, _ in current_batch])
                    visitor_ids = cursor.lastrowid

                    cursor.executemany('INSERT INTO requests (visitor_id, url, is_static, http_status, request_date) VALUES (%s, %s, %s, %s, %s)', [(visitor_ids, url, is_static, status, request_date) for ip, url, is_static, status, request_date, _, _ in current_batch])
                    
                    cursor.executemany('INSERT IGNORE INTO operating_systems (name) VALUES (%s)', [(operating_system,) for _, _, _, _, _, operating_system, _ in current_batch])
                    
                    cursor.executemany('INSERT IGNORE INTO browsers (name) VALUES (%s)', [(browser,) for _, _, _, _, _, _, browser in current_batch])
                    
                    cursor.executemany('INSERT INTO not_found_urls (url, request_date) VALUES (%s, %s)', [(url, request_date) for _, url, _, _, request_date, _, _ in current_batch if status == 404])

                    connection.commit()
                    current_batch = []

except Exception as e:
    print("Une erreur s'est produite:", e)
    connection.rollback()

finally:
    cursor.close()
    connection.close()
