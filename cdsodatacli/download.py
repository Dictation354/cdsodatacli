import subprocess
import requests
import logging
from tqdm import tqdm
import datetime
import time
import os
import random
import pandas as pd
import geopandas as gpd
from requests.exceptions import ChunkedEncodingError
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import shutil  # [修改点 1] 引入 shutil 用于跨平台文件移动
import traceback

from cdsodatacli.fetch_access_token import (
    get_bearer_access_token,
    remove_semaphore_token_file,
    MAX_VALIDITY_ACCESS_TOKEN,
    get_list_of_exising_token,
)
from cdsodatacli.session import (
    remove_semaphore_session_file,
    get_sessions_download_available,
    MAX_SESSION_PER_ACCOUNT,
)
from cdsodatacli.query import fetch_data
from cdsodatacli.utils import (
    get_conf,
    check_safe_in_archive,
    check_safe_in_spool,
    check_safe_in_outputdir,
)
from cdsodatacli.product_parser import ExplodeSAFE
from collections import defaultdict

# chunksize = 4096
chunksize = 8192  # like in the CDSE example


def CDS_Odata_download_one_product_v2(
    session,
    headers,
    url,
    output_filepath,
    semaphore_token_file,
    cdsodatacli_conf_file=None,
):
    """
     v2 is without tqdm
    Parameters
    ----------
    session (request Obj)
    headers (dict)
    url (str)
    output_filepath (str): full path where to store fetch file
    semaphore_token_file (str): full path of the file storing an active access token
    cdsodatacli_conf_file (str): path to the cdsodatacli configuration file

    Returns
    -------
        speed (float): download speed in Mo/second

    """
    speed = np.nan
    status_meaning = "unknown_code"
    t0 = time.time()
    
    conf = get_conf(path_config_file=cdsodatacli_conf_file)
    output_filepath_tmp = os.path.join(
        conf["pre_spool"], os.path.basename(output_filepath) + ".tmp"
    )
    safename_base = os.path.basename(output_filepath).replace(".zip", "")
    
    # 确保 pre_spool 存在
    if not os.path.exists(conf["pre_spool"]):
        os.makedirs(conf["pre_spool"], exist_ok=True)

    status = 0
    try:
        with open(output_filepath_tmp, "wb") as f:
            logging.debug("Downloading %s" % output_filepath)
            # [修改点 2] 添加 timeout 防止挂起 (连接超时30s, 读取超时600s)
            response = session.get(url, headers=headers, stream=True, timeout=(30, 600))
            
            status = response.status_code
            status_meaning = response.reason
            # Check for 'Transfer-Encoding: chunked'
            if (
                "Transfer-Encoding" in response.headers
                and response.headers["Transfer-Encoding"] == "chunked"
            ):
                logging.warning(
                    "Server is using 'Transfer-Encoding: chunked'. Content length may not be accurate."
                )
            if response.ok:
                total_length = int(
                    int(response.headers.get("content-length", 0)) / 1000 / 1000
                )
                logging.debug("total_length : %s Mo", total_length)
                try:
                    for chunk in response.iter_content(chunk_size=chunksize):
                        if chunk:
                            f.write(chunk)
                except ChunkedEncodingError:
                    status = -1
                    status_meaning = "ChunkedEncodingError"
                except Exception as e:
                    status = -1
                    status_meaning = f"StreamError: {str(e)}"
                    logging.error(f"Error streaming content: {e}")

    except Exception as e:
        status = -1
        status_meaning = f"RequestError: {str(e)}"
        logging.error(f"Error executing request: {e}")

    # 如果下载失败，清理临时文件
    if (status != 200 or status == -1) and os.path.exists(output_filepath_tmp):
        logging.debug("remove empty/error file %s", output_filepath_tmp)
        try:
            os.remove(output_filepath_tmp)
        except OSError:
            pass

    elapsed_time = time.time() - t0
    
    if status == 200:  # means OK download
        if elapsed_time > 0:
            speed = total_length / elapsed_time
        else:
            speed = 0
            
        # [修改点 1] 使用 shutil.move 替代 subprocess 调用 mv
        try:
            shutil.move(output_filepath_tmp, output_filepath)
            # status = subprocess.check_output(
            #     "mv " + output_filepath_tmp + " " + output_filepath, shell=True
            # )
            logging.debug("move successful: %s -> %s", output_filepath_tmp, output_filepath)
            os.chmod(output_filepath, 0o0775)
        except Exception as e:
            logging.error(f"Error moving file: {e}")
            status_meaning = "MoveFileError"
            status = -2

    logging.debug("time to download this product: %1.1f sec", elapsed_time)
    logging.debug("average download speed: %1.1fMo/sec", speed)
    return speed, status_meaning, safename_base, semaphore_token_file


def filter_product_already_present(
    cpt, df, outputdir, cdsodatacli_conf, force_download=False
):
    """
    Based on a dataframe of products to download, filter those already present locally.
    """

    all_output_filepath = []
    all_urls_to_download = []
    index_to_download = []
    for ii, safename_product in enumerate(df["safe"]):
        to_download = False
        if force_download:
            to_download = True
        if check_safe_in_archive(safename=safename_product, conf=cdsodatacli_conf):
            cpt["archived_product"] += 1
        elif check_safe_in_spool(safename=safename_product, conf=cdsodatacli_conf):
            cpt["in_spool_product"] += 1
        elif check_safe_in_outputdir(outputdir=outputdir, safename=safename_product):
            cpt["in_outdir_product"] += 1
        else:
            to_download = True
            cpt["product_absent_from_local_disks"] += 1
        if to_download:
            index_to_download.append(ii)
            id_product = df["id"].iloc[ii]
            url_product = cdsodatacli_conf["URL_download"] % id_product

            logging.debug("url_product : %s", url_product)
            logging.debug(
                "id_product : %s safename_product : %s",
                id_product,
                safename_product,
            )

            output_filepath = os.path.join(outputdir, safename_product + ".zip")
            all_output_filepath.append(output_filepath)
            all_urls_to_download.append(url_product)
    df_todownload = df.iloc[index_to_download].copy()
    df_todownload["urls"] = all_urls_to_download
    df_todownload["outputpath"] = all_output_filepath
    return df_todownload, cpt


def download_list_product_multithread_v2(
    list_id,
    list_safename,
    outputdir,
    hideProgressBar=False,
    account_group="logins",
    check_on_disk=True,
    cdsodatacli_conf_file=None,
):
    """
    v2 is handling multi account round-robin and token semaphore files
    Parameters
    ----------
    list_id (list): product hash
    list_safename (list): product names
    outputdir (str): the directory where to store the product collected
    hideProgressBar (bool): True -> no tqdm progress bar in stdout
    account_group (str): the name of the group of CDSE logins to be used
    check_on_disk (bool): True -> if the product is in the spool dir or in archive dir the download is skipped
    cdsodatacli_conf_file (str): path to the cdsodatacli configuration file

    Returns
    -------
        df2 (pd.DataFrame):
    """
    assert len(list_id) == len(list_safename)
    logging.info("check_on_disk : %s", check_on_disk)
    cpt = defaultdict(int)
    cpt["products_in_initial_listing"] = len(list_id)
    conf = get_conf(path_config_file=cdsodatacli_conf_file)
    if hideProgressBar:
        os.environ["DISABLE_TQDM"] = "True"
    all_speeds = []
    # status, 0->not treated, -1->error download , 1-> successful download
    df = pd.DataFrame(
        {"safe": list_safename, "status": np.zeros(len(list_safename)), "id": list_id}
    )
    force_download = not check_on_disk
    df2, cpt = filter_product_already_present(
        cpt, df, outputdir, force_download=force_download, cdsodatacli_conf=conf
    )

    logging.info("%s", cpt)
    while_loop = 0
    blacklist = []
    while (df2["status"] == 0).any():

        while_loop += 1
        subset_to_treat = df2[df2["status"] == 0]
        dfproductDownloaddable = get_sessions_download_available(
            conf,
            subset_to_treat,
            hideProgressBar=True,
            blacklist=blacklist,
            logins_group=account_group,
        )
        
        if len(dfproductDownloaddable) == 0:
            logging.info("No more sessions/accounts available. Waiting...")
            time.sleep(10)
            # Break to avoid infinite loop if really no accounts are working
            # Or continue if waiting for tokens to expire. 
            # Ideally logic should be smarter here, but original code would just log and break inner loops.
            if len(blacklist) >= len(conf[account_group]):
                 logging.error("All accounts blacklisted. Stopping.")
                 break
            continue

        logging.info(
            "while_loop : %s, prod. to treat: %s, slot avail.:%s, %s",
            while_loop,
            len(subset_to_treat),
            len(dfproductDownloaddable),
            cpt,
        )
        with (
            ThreadPoolExecutor(max_workers=len(dfproductDownloaddable)) as executor,
            tqdm(total=len(dfproductDownloaddable)) as pbar,
        ):
            future_to_url = {
                executor.submit(
                    CDS_Odata_download_one_product_v2,
                    dfproductDownloaddable["session"].iloc[jj],
                    dfproductDownloaddable["header"].iloc[jj],
                    dfproductDownloaddable["url"].iloc[jj],
                    dfproductDownloaddable["output_path"].iloc[jj],
                    dfproductDownloaddable["token_semaphore"][jj],
                    cdsodatacli_conf_file=cdsodatacli_conf_file,
                ): (jj)
                for jj in range(len(dfproductDownloaddable))
            }
            errors_per_account = defaultdict(int)
            for future in as_completed(future_to_url):
                # [修改点 3] 增加 Try-Except 块捕获线程异常
                try:
                    (
                        speed,
                        status_meaning,
                        safename_base,
                        semaphore_token_file,
                    ) = future.result()
                    
                    # remove semaphore once the download is over (successful or not)
                    login = os.path.basename(semaphore_token_file).split("_")[3]
                    date_generation_access_token = datetime.datetime.strptime(
                        os.path.basename(semaphore_token_file)
                        .split("_")[4]
                        .replace(".txt", ""),
                        "%Y%m%dt%H%M%S",
                    )

                    remove_semaphore_token_file(
                        token_dir=conf["token_directory"],
                        login=login,
                        date_generation_access_token=date_generation_access_token,
                    )
                    logging.info("remove session semaphore for %s", login)
                    remove_semaphore_session_file(
                        session_dir=conf["active_session_directory"],
                        safename=safename_base,
                        login=login,
                    )

                    if status_meaning == "OK":
                        df2.loc[(df2["safe"] == safename_base), "status"] = 1
                        all_speeds.append(speed)
                        cpt["successful_download"] += 1
                    else:
                        df2.loc[(df2["safe"] == safename_base), "status"] = -1
                        errors_per_account[login] += 1
                        logging.info("error found for %s meaning %s", login, status_meaning)
                    
                    cpt["status_%s" % status_meaning] += 1

                except Exception as e:
                    # 获取该 future 对应的索引，以便清理资源
                    jj = future_to_url[future]
                    safename_err = dfproductDownloaddable["safe"].iloc[jj]
                    session_sem_err = dfproductDownloaddable["session_semaphore"].iloc[jj]
                    
                    logging.error(f"Critical error in download thread for {safename_err}: {e}")
                    logging.error(traceback.format_exc())
                    
                    # 标记为失败
                    df2.loc[(df2["safe"] == safename_err), "status"] = -1
                    cpt["status_Exception"] += 1
                    
                    # 尝试清理 session semaphore 以释放插槽
                    if os.path.exists(session_sem_err):
                        try:
                            os.remove(session_sem_err)
                            logging.debug(f"Cleaned up session semaphore for failed download: {session_sem_err}")
                        except OSError:
                            pass

                pbar.update(1)
            
            for acco in errors_per_account:
                if errors_per_account[acco] >= MAX_SESSION_PER_ACCOUNT:
                    blacklist.append(acco)
                    logging.info("%s black listed for next loops", acco)
                    
    logging.info("download over.")
    logging.info("counter: %s", cpt)
    # safety remove active session, all reamining because of error
    remove_semaphore_session_file(
        session_dir=conf["active_session_directory"],
        safename=None,
        login=None,
    )

    if len(all_speeds) > 0:
        logging.info(
            "average download speed %1.1f Mo/s (stdev: %1.1f Mo/s)",
            np.mean(all_speeds),
            np.std(all_speeds),
        )
    return df2


def download_list_product(
    list_id,
    list_safename,
    outputdir,
    specific_account,
    specific_passwd=None,
    hideProgressBar=False,
    cdsodatacli_conf_file=None,
):
    """
    Sequential download (Simplified for context)
    """
    assert len(list_id) == len(list_safename)
    conf = get_conf(path_config_file=cdsodatacli_conf_file)
    cpt = defaultdict(int)
    all_speeds = []
    cpt["products_in_initial_listing"] = len(list_id)
    lst_usable_tokens = get_list_of_exising_token(token_dir=conf["token_directory"])
    if lst_usable_tokens == []:  # in case no token ready to be used -> create new one
        (
            access_token,
            date_generation_access_token,
            login,
            path_semphore_token,
        ) = get_bearer_access_token(
            quiet=hideProgressBar,
            specific_account=specific_account,
            passwd=specific_passwd,
        )
    else:  # select randomly one token among existing
        path_semphore_token = random.choice(lst_usable_tokens)
        date_generation_access_token = datetime.datetime.strptime(
            os.path.basename(path_semphore_token).split("_")[4].replace(".txt", ""),
            "%Y%m%dt%H%M%S",
        )
        access_token = open(path_semphore_token).readlines()[0]
    if access_token is not None:
        headers = {"Authorization": "Bearer %s" % access_token}
        logging.debug("headers: %s", headers)
        session = requests.Session()
        session.headers.update(headers)
        if hideProgressBar:
            os.environ["DISABLE_TQDM"] = "True"

        pbar = tqdm(
            range(len(list_id)), disable=bool(os.environ.get("DISABLE_TQDM", False))
        )
        for ii in pbar:
            pbar.set_description("CDSE download %s" % cpt)
            id_product = list_id[ii]
            url_product = conf["URL_download"] % id_product
            safename_product = list_safename[ii]
            if check_safe_in_archive(safename=safename_product, conf=conf):
                cpt["archived_product"] += 1
            elif check_safe_in_spool(safename=safename_product, conf=conf):
                cpt["in_spool_product"] += 1
            else:
                cpt["product_absent_from_local_disks"] += 1

                logging.debug("url_product : %s", url_product)
                logging.debug(
                    "id_product : %s safename_product : %s",
                    id_product,
                    safename_product,
                )
                if (
                    datetime.datetime.today() - date_generation_access_token
                ).total_seconds() >= MAX_VALIDITY_ACCESS_TOKEN:
                    logging.info("get a new access token")
                    (
                        access_token,
                        date_generation_access_token,
                        specific_account,
                        path_semphore_token,
                    ) = get_bearer_access_token(specific_account=specific_account)
                    headers = {"Authorization": "Bearer %s" % access_token}
                    session.headers.update(headers)
                else:
                    logging.debug("reuse same access token, still valid.")
                output_filepath = os.path.join(outputdir, safename_product + ".zip")

                (
                    speed,
                    status_meaning,
                    safename_base,
                    path_semphore_token,
                ) = CDS_Odata_download_one_product_v2(
                    session,
                    headers,
                    url=url_product,
                    output_filepath=output_filepath,
                    semaphore_token_file=path_semphore_token,
                    cdsodatacli_conf_file=cdsodatacli_conf_file,
                )
                remove_semaphore_token_file(
                    token_dir=conf["token_directory"],
                    login=specific_account,
                    date_generation_access_token=date_generation_access_token,
                )
                remove_semaphore_session_file(
                    session_dir=conf["active_session_directory"],
                    safename=safename_base,
                    login=specific_account,
                )
                if status_meaning == "OK":
                    all_speeds.append(speed)
                    cpt["successful_download"] += 1
                cpt["status_%s" % status_meaning] += 1
                
    logging.info("download over.")
    logging.info("counter: %s", cpt)
    if len(all_speeds) > 0:
        logging.info(
            "average download speed %1.1f Mo/s (stdev: %1.1f Mo/s)",
            np.mean(all_speeds),
            np.std(all_speeds),
        )


def main():
    """
    download data from an existing listing of product
    """
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)

    import argparse

    parser = argparse.ArgumentParser(description="download-from-CDS")
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument(
        "--hideProgressBar",
        action="store_true",
        default=False,
        help="hide the tqdm progress bar for each prodict download",
    )
    parser.add_argument(
        "--listing",
        required=True,
        help="list of product to treat csv files id,safename",
    )
    parser.add_argument(
        "--login",
        required=True,
        help="CDSE account to be used for download (email address)",
    )
    parser.add_argument(
        "--outputdir",
        required=True,
        help="directory where to store fetch files",
    )
    parser.add_argument(
        "--cdsodatacli_conf_file",
        required=False,
        default=None,
        help="path to the cdsodatacli configuration file .yml",
    )

    args = parser.parse_args()
    fmt = "%(asctime)s %(levelname)s %(filename)s(%(lineno)d) %(message)s"
    if args.verbose:
        logging.basicConfig(
            level=logging.DEBUG, format=fmt, datefmt="%d/%m/%Y %H:%M:%S", force=True
        )
    else:
        logging.basicConfig(
            level=logging.INFO, format=fmt, datefmt="%d/%m/%Y %H:%M:%S", force=True
        )
    t0 = time.time()
    inputdf = pd.read_csv(args.listing, names=["id", "safename"], delimiter=",")
    if not os.path.exists(args.outputdir):
        logging.debug("mkdir on %s", args.outputdir)
        os.makedirs(args.outputdir, 0o0775)
    download_list_product(
        list_id=inputdf["id"].values,
        list_safename=inputdf["safename"].values,
        outputdir=args.outputdir,
        hideProgressBar=args.hideProgressBar,
        specific_account=args.login,
        cdsodatacli_conf_file=args.cdsodatacli_conf_file,
    )
    elapsed = t0 - time.time()
    logging.info("end of function in %s seconds", elapsed)