import os
import gzip
import pygeoip
import time
import datetime
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import user_agents
import csv
import cProfile
import sys
import sqlite3


class Yieldify:

    def __init__(self, folder_output_path, folder_log_path, localisation_path):
        self.folder_output_path = folder_output_path
        self.folder_log_path = folder_log_path
        self.localisation_data = pygeoip.GeoIP(localisation_path)

    def log_error(self, error_type=""):
        with open(os.path.join(self.folder_log_path, 'ErrorLog.txt'), 'ab') as error_log_file:
            f = csv.writer(error_log_file)
            f.writerow(["Type", error_type, "Path", self.raw_file_path, "Line", self.line_nb, "Full line", self.line])
        self.error = True
        self.file_process_sucess = False

    def log_sucess(self):
        with open(os.path.join(self.folder_log_path, 'SucessLog.txt'), 'a') as sucess_log_file:
            sucess_log_file.write(self.raw_file_path + "\n")

    def get_localisation_from_ip(self):
        self.ip = None
        ips = self.ips.replace(" ", "")
        ips_in_list = ips.split(',')
        for ip in ips_in_list:
            try:
                data = self.localisation_data.record_by_name(ip)
                self.ip = ip
                self.country = data['country_name']
                self.city = data['city']
                self.longitude = data['longitude']
                self.latitude = data['latitude']
                break
            except:
                continue
        if self.ip is None:
            self.latitude = None
            self.longitude = None
            self.country = None
            self.city = None
            self.log_error(error_type="Localisation from ip")

    def get_ua_data(self):
        try:
            self.ua = user_agents.parse(self.ua_string)
            self.user_browser = self.ua.browser.family
            self.user_os = self.ua.os.family
            self.is_mobile = self.ua.is_mobile
        except:
            self.ua = None
            self.user_browser = None
            self.user_os = None
            self.is_mobile = None
            self.log_error(error_type="User Agent data")

    def get_timestamp(self):
        try:
            self.datetime = self.date + " " + self.time
            current_datetime = datetime.datetime.strptime(self.datetime, "%Y-%m-%d %H:%M:%S").timetuple()
            self.timestamp = time.mktime(current_datetime)
        except:
            self.datetime = None
            self.timestamp = None
            self.log_error(error_type="Timestamp")

    def get_new_path(self):
        try:
            date_in_list = self.date.split("-")
            year, month, day = date_in_list[0], date_in_list[1], date_in_list[2]
            self.basename = os.path.basename(self.raw_file_path).split('.')[0]
            self.json_file_path = os.path.join(self.folder_output_path, year, month, day, self.basename + ".json.gz")
            self.json_file_dir = os.path.dirname(self.json_file_path)
        except:
            self.json_file_path = None
            self.json_file_dir = None
            self.log_error(error_type="New file path")

    def get_json_dict(self):
        self.json_dict = {"url": self.url,
                          "timestamp": self.timestamp,
                          "user_id": self.user_id,
                          "ip": self.ips,
                          "location": {"latitude": self.latitude,
                                       "longitude": self.longitude,
                                       "country": self.country,
                                       "city": self.city
                                       },
                          "user_agent": {"mobile": self.is_mobile,
                                         "os_family": self.user_os,
                                         "string": self.ua_string,
                                         "browser_family": self.user_browser
                                         }
                          }

    def process_line(self):
        self.line_in_list = self.line.split("\t")
        if len(self.line_in_list) != 6:
            self.line_in_list = None
            self.log_error(error_type="Line length")
        else:
            self.date = self.line_in_list[0]
            self.time = self.line_in_list[1]
            self.user_id = self.line_in_list[2]
            self.url = self.line_in_list[3]
            self.ips = self.line_in_list[4]
            self.ua_string = self.line_in_list[5]
            self.get_ua_data()
            self.get_localisation_from_ip()
            self.get_timestamp()
            self.get_new_path()
            self.get_json_dict()
            self.add_to_db()

    def process_file(self, raw_file_path):
        self.file_process_sucess = True  # Can be use to log in spite of some error
        self.raw_file_path = raw_file_path
        with gzip.open(self.raw_file_path, 'rb') as raw_file:
            self.line_nb = 0
            self.line = raw_file.readline()[:-1]
            while self.line != "":
                self.error = False
                self.process_line()
                if not self.error:
                    if not os.path.isdir(os.path.dirname(self.json_file_path)):
                        os.makedirs(os.path.dirname(self.json_file_path))
                    with gzip.open(self.json_file_path, "a") as json_file:
                        json_file.write(str(self.json_dict) + "\n")
                self.line = raw_file.readline()[:-1]
        if self.file_process_sucess:
            self.log_sucess()

    def process_folder(self, folder_input_path):
        print "Start processing %s" % folder_input_path
        self.folder_input_path = folder_input_path
        for current_path, subdirs, file_names in os.walk(self.folder_input_path):
            for raw_file_name in file_names:
                self.process_file(os.path.join(current_path, raw_file_name))
        print "Folder %s was fully processed" % folder_input_path

    def add_to_db(self):
        pass

class WatchdogHandler(PatternMatchingEventHandler):
    patterns = ["*.gz"]

    def __init__(self, folder_output_path, folder_log_path, localisation_path):
        PatternMatchingEventHandler.__init__(self)
        self.yieldify = Yieldify(folder_output_path, folder_log_path, localisation_path)

    def on_created(self, event):
        print "Start processing new file: " + event.src_path
        time.sleep(1)  # Wait to be sure the file is not in use
        self.yieldify.process_file(event.src_path)


def watch_folder(folder_input_path, folder_output_path, folder_log_path, localisation_path):
    observer = Observer()
    observer.schedule(WatchdogHandler(folder_output_path, folder_log_path, localisation_path), path=folder_input_path, recursive=True)
    observer.start()
    print "Observer started on %s" % folder_input_path
    print "Press Ctrl + C to stop"
    i = 0
    try:
        while True:
            time.sleep(0.5)
            i += 1
            sys.stdout.write("\rRunning [" + " "*(i%10)+">"+" "*(9-i%10) + "]")
            sys.stdout.flush()
    except:
        observer.stop()
        print "Observer stopped"
    observer.join()


if __name__ == "__main__":
    folder_input_path = "C:\Users\Alfred\Documents\AWS\Yieldify\data"
    folder_output_path = "C:\Users\Alfred\Documents\AWS\Yieldify\processed\Alfred"
    folder_log_path = "C:\Users\Alfred\Documents\AWS\Yieldify\processed\Alfred"
    localisation_path = "GeoLiteCity.dat"
    yieldify = Yieldify(folder_output_path, folder_log_path, localisation_path)
    # cProfile.run("yieldify.process_folder(folder_input_path)")
    yieldify.process_folder(folder_input_path)
    watch_folder(folder_input_path, folder_output_path, folder_log_path, localisation_path)


