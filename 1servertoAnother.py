import os
import pandas as pd
from pytube import YouTube
from urllib.request import urlopen
import logging
import ssl
import pandas as pd
import os
import random
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from time import sleep
import paramiko
import shutil
from decouple import config

# PREFIX = "yt_2600_server"

# SERVER = "110"















PROXIES = [
    '144.217.197.151:39399',
    '157.230.9.235:3128',
    '3.99.201.168:3128',
    '129.153.150.87:80',
    '54.36.26.122:80'
]
context=ssl._create_unverified_context()
SENT_DIRECTORY = os.path.join(os.getcwd(), "SENT")
DOWNLOAD_DIRECTORY = os.path.join(os.getcwd(), "94K_VIDOS")
TOTAL_DIRECTORY=os.path.join(os.getcwd(),"TOTAL_DOWNLOAD")
# SENT_DIRECTORY = os.path.join(os.getcwd(), "SENT")


servers = [
    "16.16.171.110",
    "16.16.199.180",
    "13.53.105.179",
    "16.16.220.43",
    "13.50.111.78",
    "16.170.170.32",
    "16.16.199.10",
    "16.171.69.192",
    "16.16.179.219",
    "13.48.128.190",
    "13.50.225.153",
    "16.170.170.156",
    "13.50.240.5",
    "13.48.132.172",
    "13.50.109.236",
    "13.49.221.219",
    ''' LIST OF SERVERS '''
]




'''SUBMAIN'''
username = 'ubuntu'
private_key_path = '/Users/atitsharma/Desktop/94K/videotest.pem'
sftp_folder = '/home/ubuntu/test94k'
# log_folder = '/home/ubuntu/23june/latest-logs'
local_folder = DOWNLOAD_DIRECTORY
port=2017
hostname="16.16.199.10"




private_key = paramiko.RSAKey.from_private_key_file(private_key_path)


def post_to_server(source_path):
    
    private_key=paramiko.RSAKey.from_private_key_file(private_key_path)
    ssh=paramiko.SSHClient()

    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=hostname, username=username, pkey=private_key)

    command ='mkdir -p /home/ubuntu/test94k/videos'  
    stdin, stdout, stderr = ssh.exec_command(command)

    # Check for any error output
    error = stderr.read().decode().strip()
    if error:
        print(f'Error creating directory: {error}')
    else:
        print('Directory created successfully')

    sftp_client=ssh.open_sftp()

    sftp_client.chdir("/home/ubuntu/test94k")

    # print(sftp_client.getcwd())
    
    files=os.listdir(source_path)
    print(files)
    for file in files:
        if file.endswith('.mp4'):
            try:
                t = paramiko.Transport((hostname,22 ))
                t.connect(username=username, pkey=private_key)
                sftp = paramiko.SFTPClient.from_transport(t)
                sftp.put(os.path.join(source_path, file),"/home/ubuntu/test94k/videos" + "/" + file)
                print("Sucessfully Imported !! ")
            finally:
                t.close()
               
    
                
    # sftp_client.get("demo.txt","/Users/atitsharma/Desktop/94K/hello.txt")
    # sftp_client.put(source_path,"/home/ubuntu/test94k")
    # sftp_client.close()
    

    sftp_client.close()
    ssh.close()
    return True





""" SSS VIDEO DOWNLOADER """
def download_age_restricted_video(url):
    chrome_options = Options()

    

    
    prefs = {
        "download.default_directory":TOTAL_DIRECTORY,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True
    }
    chrome_options.add_experimental_option("prefs", prefs)

  
    chrome_options.add_argument("--headless")
   
    proxy = random.choice(PROXIES)
    logger.info(f"Using proxy: {proxy}")
    driver = webdriver.Chrome(options=chrome_options)
    chrome_options.add_argument('--proxy-server=%s' % proxy)
    driver.get("https://en.savefrom.net/391GA/")
    
    search = WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.ID, "sf_url")))
    search.send_keys(url)
    search.send_keys(Keys.RETURN)

    count = 0
    try:
        for attempt in range(30):
            try:
                download_button = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".link.link-download.subname.ga_track_events.download-icon"))
                )
                logger.info(f"{url} Downloading Age Restricted video Attempt {attempt+1}/30")
                sleep(5) 
                download_button.click()
                sleep(5)
                logger.info(f"{url} Downloaded Successfully in {attempt+1}/30 attempts using SSfromNet")
    
                files = os.listdir(TOTAL_DIRECTORY)
                # print(files)
                # print(list(files))
                for file_name in files:
                    source_file = os.path.join(TOTAL_DIRECTORY, file_name)
                    destination_file1 = os.path.join(SENT_DIRECTORY, file_name)
                    destination_file2= os.path.join(DOWNLOAD_DIRECTORY, file_name)
                    shutil.copy2(source_file, destination_file1)    
                    shutil.copy2(source_file, destination_file2) 
                    # shutil.rmtree(TOTAL_DIRECTORY)         
                driver.quit()
                return "success"
            except Exception as e:
                logger.error(f"{url} Atttempt: {attempt}, Exception: {e}")
                count += 1
                if count >= 31:
                    logger.error(f"{url} Unable to download video: {e}")
                    driver.quit()
                    return e
                driver.quit()
            finally:
                driver.quit()
    finally:
        driver.quit()

  
  
  
  

'''PYTUBE'''
def download_youtube_video(url, output_path,output_path2):
    try:
        youtube = YouTube(url)
        video = youtube.streams.get_lowest_resolution()
        video.download(output_path)
        video.download(output_path2)
        
        logger.info(f"Downoladed Successfully Using Pytube {url}!!!")
        return "Success"

    except Exception as e:
        
        if "age restricted" in str(e):
            status=download_age_restricted_video(url)
            if status:
                logger.info(f"{url} Age Restricted Video downloded Successfully !!!")
                return "Success"
        logger.info(f"Unable to dowload video {url} {e}")
        return e
    





''' READ EXCEL'''
def download_videos_from_excel(file_path):

    df = pd.read_excel(file_path)

    os.makedirs(DOWNLOAD_DIRECTORY, exist_ok=True)
    os.makedirs(SENT_DIRECTORY, exist_ok=True)
    os.makedirs(TOTAL_DIRECTORY, exist_ok=True)
    
    for index, row in df.iterrows():
        url = row["video_url"]
        status = download_youtube_video(url, DOWNLOAD_DIRECTORY,SENT_DIRECTORY)     
        df.at[index, "dld_status"] = status
        output_file_path = file_path 
        print(index)
        if (index+1)%10==0:
            df.to_excel(output_file_path, index=False) 
            files = os.listdir(SENT_DIRECTORY)
            push_status=post_to_server(SENT_DIRECTORY)
            print(push_status)
            shutil.rmtree(SENT_DIRECTORY)
            shutil.rmtree(TOTAL_DIRECTORY)
            os.makedirs(SENT_DIRECTORY, exist_ok=True)
            os.makedirs(TOTAL_DIRECTORY, exist_ok=True)  
                
            

            logger.info(f"Downloded video  {index+1}/{len(df)}")
            logger.info("Successfully Updated in excel ")
    df.to_excel(output_file_path, index=False) 
    logger.info(" PROCESS SUCESSFULL !!! ")
        


'''MAIN'''
if __name__ =='__main__':
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        filename="logs/YoutubeDownloadLog.log",
        filemode="w",
        format="%(module)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)
    logger.setLevel("INFO")
    excel_file_path = "Excels/dataframe_1.xlsx"
    download_videos_from_excel(excel_file_path)
    
    

