from flask import Flask, render_template
import pymysql
import pandas as pd
import matplotlib.pyplot as plt
import folium

app = Flask(__name__)

def get_db_connection():
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='taha123',
                                 database='logApa')
    return connection

@app.route('/')
def index():
    connection = get_db_connection()
    cursor = connection.cursor()

    cursor.execute('SELECT COUNT(DISTINCT ip) FROM visitors')
    unique_visitors = cursor.fetchone()[0]

    cursor.execute('SELECT COUNT(*) FROM requests')
    total_requests = cursor.fetchone()[0]

    cursor.execute('SELECT url, COUNT(*) FROM requests GROUP BY url ORDER BY COUNT(*) DESC LIMIT 10')
    top_urls = cursor.fetchall()

    cursor.execute('SELECT url FROM not_found_urls')
    not_found_urls = cursor.fetchall()

    cursor.execute('SELECT ip, hostname FROM visitors')
    visitors_info = cursor.fetchall()

    cursor.execute('SELECT geo_location FROM visitors')
    geo_locations = cursor.fetchall()

    cursor.execute('SELECT code, COUNT(*) FROM requests JOIN http_status ON requests.http_status = http_status.id GROUP BY code')
    http_status_counts = cursor.fetchall()

    cursor.execute('SELECT operating_systems.name, COUNT(*) FROM requests JOIN operating_systems ON requests.operating_system_id = operating_systems.id GROUP BY operating_systems.name')
    os_counts = cursor.fetchall()

    cursor.execute('SELECT browsers.name, COUNT(*) FROM requests JOIN browsers ON requests.browser_id = browsers.id GROUP BY browsers.name')
    browser_counts = cursor.fetchall()

    cursor.execute('SELECT visitors.ip, browsers.name AS browser, visitors.geo_location FROM requests JOIN visitors ON requests.visitor_id = visitors.id JOIN browsers ON requests.browser_id = browsers.id')
    ip_browser_geo = cursor.fetchall()

    df_http_status = pd.DataFrame(http_status_counts, columns=['HTTP Status', 'Count'])
    df_os = pd.DataFrame(os_counts, columns=['Operating System', 'Count'])
    df_browser = pd.DataFrame(browser_counts, columns=['Browser', 'Count'])

    plt.figure(figsize=(15, 5))

    plt.subplot(1, 3, 1)
    plt.bar(df_http_status['HTTP Status'], df_http_status['Count'])
    plt.title('HTTP Status Codes')
    plt.xlabel('HTTP Status Code')
    plt.ylabel('Count')

    plt.subplot(1, 3, 2)
    plt.bar(df_os['Operating System'], df_os['Count'])
    plt.title('Operating Systems')
    plt.xlabel('Operating System')
    plt.ylabel('Count')
    plt.xticks(rotation=45)

    plt.subplot(1, 3, 3)
    plt.bar(df_browser['Browser'], df_browser['Count'])
    plt.title('Browsers')
    plt.xlabel('Browser')
    plt.ylabel('Count')
    plt.xticks(rotation=45)

    plt.tight_layout()
    plt.savefig('static/graphs.png')

    # Create a map
    map = folium.Map(location=[0, 0], zoom_start=2)
    for ip, browser, geo in ip_browser_geo:
        if geo:
            lat, lon = map(float, geo.split(','))
            folium.Marker(location=[lat, lon], popup=f'IP: {ip}, Browser: {browser}').add_to(map)
    map.save('templates/map.html')

    connection.close()

    return render_template('index.html', unique_visitors=unique_visitors, total_requests=total_requests,
                           top_urls=top_urls, not_found_urls=not_found_urls, visitors_info=visitors_info,
                           geo_locations=geo_locations, ip_browser_geo=ip_browser_geo)

if __name__ == '__main__':
    app.run(debug=True)
