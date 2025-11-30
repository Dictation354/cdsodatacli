from cdsodatacli.utils import get_conf
import requests
import logging
import json
import datetime
import os
import glob
import random

MAX_VALIDITY_ACCESS_TOKEN = 600  # sec (defined by CDS API)


def get_bearer_access_token(
    quiet=True,
    specific_account=None,
    passwd=None,
    account_group="logins",
    path_config_file=None,
    conf=None,
):
    """
    OData access token (validity=600sec)

    Parameters
    ----------
    quiet (bool): True -> curl in silent mode (kept for compatibility, though not used with requests)
    specific_account (str) [optional, default=None -> first available account in config file]
    passwd (str): [optional, default is to search in config files]
    account_group (str): name of the group of accounts in the config file [default='logins']
    path_config_file (str): path to the configuration file [optional, default is None -> localconfig.yml or config.yml]
    conf (dict): configuration dictionary [optional, default=None]

    Returns
    -------
        token (str): access token
        date_generation_access_token (datetime.datetime): date of generation of the token
        login (str): CDSE account used
        path_semphore_token (str): path of the semaphore file created to store the token

    """
    if conf is None:
        conf = get_conf(path_config_file=path_config_file)
        
    path_semphore_token = None
    url_identity = conf["URL_identity"]
    
    # 从配置读取 SSL 设置，默认为 True
    verify_ssl = conf.get("ssl_verify", True)

    if specific_account is None:
        all_accounts = list(conf[account_group].keys())
        if not all_accounts:
            logging.error(f"No accounts found in group '{account_group}'")
            return None, None, None, None
        login = random.choice(all_accounts)
        if passwd is None:
            passwd = conf[account_group][all_accounts[0]]
    else:
        login = specific_account
        if passwd is None:
            logging.debug("conf[account_group] %s", type(conf[account_group]))
            passwd = conf[account_group].get(specific_account)
            if passwd is None:
                logging.error(f"Password for account {login} not found in config.")
                return None, None, None, None

    # 构造请求数据
    payload = {
        "client_id": "cdse-public",
        "username": login,
        "password": passwd,
        "grant_type": "password",
    }
    
    logging.debug(f"Requesting token for user: {login}")
    date_generation_access_token = datetime.datetime.today()
    
    try:
        # 使用 requests 发送 POST 请求
        response = requests.post(
            url_identity, 
            data=payload, 
            verify=verify_ssl,
            timeout=30 # 设置超时防止挂起
        )
        
        # 检查 HTTP 响应状态码
        response.raise_for_status()
        
        data = response.json()
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching token for {login}: {e}")
        # 如果有响应内容，尝试打印出来以便调试
        if 'response' in locals() and response is not None:
             logging.error(f"Server response: {response.text}")
        return None, None, None, None
    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON response from {url_identity}")
        return None, None, None, None

    if "access_token" not in data:
        logging.info(
            "Authentication failed or unexpected response for account (%s): %s", login, data
        )
        token = None
    else:
        token = data["access_token"]
        path_semphore_token = write_token_semphore_file(
            login=login,
            date_generation_access_token=date_generation_access_token,
            token_dir=conf["token_directory"],
            access_token=token,
        )
    return token, date_generation_access_token, login, path_semphore_token


def write_token_semphore_file(
    login, date_generation_access_token, token_dir, access_token
):
    """
    Parameters
    ----------
    login (str) :email address of CDSE account
    date_generation_access_token (datetime.datetime)
    token_dir (str)
    access_token (str)

    Returns
    -------
    path_semphore_token (str)
    """
    # 确保 token 目录存在
    if not os.path.exists(token_dir):
        try:
            os.makedirs(token_dir, exist_ok=True)
        except OSError as e:
            logging.error(f"Failed to create token directory {token_dir}: {e}")

    path_semphore_token = os.path.join(
        token_dir,
        "CDSE_access_token_%s_%s.txt"
        % (login, date_generation_access_token.strftime("%Y%m%dt%H%M%S")),
    )
    try:
        with open(path_semphore_token, "w") as fid:
            fid.write(access_token)
    except IOError as e:
        logging.error(f"Failed to write token file {path_semphore_token}: {e}")
        return None
        
    return path_semphore_token


def get_list_of_exising_token(token_dir, account=None):
    """
    Parameters
        token_dir (str)
        account (str): optional

    Returns
    -------
        lst_token (list)
    """
    if account is not None:
        lst_token0 = glob.glob(
            os.path.join(token_dir, "CDSE_access_token_%s_*.txt" % account)
        )
    else:
        lst_token0 = glob.glob(os.path.join(token_dir, "CDSE_access_token_*.txt"))

    lst_token = []
    for ll in lst_token0:
        try:
            # 增加解析安全性
            filename = os.path.basename(ll)
            # 假设文件名格式为 CDSE_access_token_EMAIL_DATETIME.txt
            # split("_")可能会有问题如果email包含下划线，但原逻辑是用索引4
            # 更加稳健的做法是取倒数第一个部分作为日期
            parts = filename.replace(".txt", "").split("_")
            if len(parts) < 2:
                continue
                
            date_str = parts[-1] 
            date_generation_access_token = datetime.datetime.strptime(
                date_str, "%Y%m%dt%H%M%S"
            )
            
            if (
                datetime.datetime.today() - date_generation_access_token
            ).total_seconds() < MAX_VALIDITY_ACCESS_TOKEN:
                lst_token.append(ll)
        except (IndexError, ValueError):
            logging.warning(f"Skipping malformed or unrelated token file: {ll}")
            continue
            
    logging.debug("Number of valid tokens found: %s", len(lst_token))
    return lst_token


def remove_semaphore_token_file(token_dir, login, date_generation_access_token):
    """
    this function is supposed to be used when a download is finished
    """
    path_token = os.path.join(
        token_dir,
        "CDSE_access_token_%s_%s.txt"
        % (login, date_generation_access_token.strftime("%Y%m%dt%H%M%S")),
    )
    exists = os.path.exists(path_token)
    if (
        exists
        and (datetime.datetime.today() - date_generation_access_token).total_seconds()
        >= MAX_VALIDITY_ACCESS_TOKEN
    ):
        try:
            os.remove(path_token)
            logging.debug("token semaphore file removed")
        except OSError:
            pass