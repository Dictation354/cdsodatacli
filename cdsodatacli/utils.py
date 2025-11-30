from yaml import load
import logging
import os
import cdsodatacli
from yaml import Loader
import datetime
import pandas as pd
import json
import zipfile  # 新增导入用于完整性检查

local_config_potential_path = os.path.join(
    os.path.dirname(cdsodatacli.__file__), "localconfig.yml"
)
config_path = os.path.join(os.path.dirname(cdsodatacli.__file__), "config.yml")


def get_conf(path_config_file=None) -> dict:
    """
    Load configuration from localconfig.yml or config.yml in cdsodatacli package directory.
    
    Priority order:
    1. path_config_file (if provided)
    2. localconfig.yml in Current Working Directory (CWD)
    3. localconfig.yml in package directory
    4. config.yml in package directory

    Args:
        path_config_file (str, optional): Full path to the configuration YAML file. Defaults to None.

    Returns:
        dict: Configuration parameters loaded from the YAML file.
    """
    # 优先检查当前工作目录下的配置
    cwd_config = os.path.join(os.getcwd(), "localconfig.yml")

    if path_config_file is not None:
        used_config_path = path_config_file
        assert os.path.exists(used_config_path), f"{used_config_path} does not exist"
    elif os.path.exists(cwd_config):
        used_config_path = cwd_config  # 命中当前目录配置
    else:
        if os.path.exists(local_config_potential_path):
            used_config_path = local_config_potential_path
        else:
            used_config_path = config_path
            
    logging.debug("config path that is used: %s", used_config_path)
    stream = open(used_config_path, "r")
    conf = load(stream, Loader=Loader)
    return conf


def check_safe_in_outputdir(outputdir, safename):
    """
    Check if the SAFE product exists in the output directory and verify its integrity if it is a zip file.

    Parameters
    ----------
    safename (str) basename

    Returns
    -------
        present_in_outputdir (bool): True -> the product is already in the output dir and valid
    """
    present_in_outdir = False
    potential_file = None
    
    # 检查可能的文件名变体
    candidates = [
        os.path.join(outputdir, safename + ".zip"),
        os.path.join(outputdir, safename),
        os.path.join(outputdir, safename.replace(".SAFE", ".zip"))
    ]

    for f in candidates:
        if os.path.exists(f):
            potential_file = f
            break
    
    if potential_file:
        # 如果是 ZIP 文件，进行完整性检查
        if potential_file.endswith(".zip"):
            try:
                with zipfile.ZipFile(potential_file, 'r') as zf:
                    # testzip 返回 None 表示无错误，否则返回第一个损坏文件的名称
                    if zf.testzip() is not None:
                        raise zipfile.BadZipFile("Corrupted file content inside zip")
                present_in_outdir = True
            except (zipfile.BadZipFile, OSError) as e:
                logging.warning(f"Found corrupted file {os.path.basename(potential_file)}, removing it: {e}")
                try:
                    os.remove(potential_file)
                except OSError:
                    pass
                present_in_outdir = False
        else:
            # 对于非 zip (如解压后的 .SAFE 目录)，暂只检查存在性
            present_in_outdir = True

    logging.debug("present_in_outdir for %s : %s", safename, present_in_outdir)
    return present_in_outdir


def check_safe_in_spool(safename, conf):
    """

    Parameters
    ----------
    safename (str) basename
    conf (dict) configuration dictionary of cdsodatacli package

    Returns
    -------
        present_in_spool (bool): True -> the product is already in the spool dir

    """
    present_in_spool = False
    for uu in ["", ".zip", "replaced"]:
        if uu == "":
            spool_potential_file = os.path.join(conf["spool"], safename)
        elif uu == ".zip":
            spool_potential_file = os.path.join(conf["spool"], safename + ".zip")
        elif uu == "replaced":
            spool_potential_file = os.path.join(
                conf["spool"], safename.replace(".SAFE", ".zip")
            )
        else:
            raise NotImplementedError
        if os.path.exists(spool_potential_file):
            present_in_spool = True
            break
    logging.debug("present_in_spool : %s", present_in_spool)
    return present_in_spool


def WhichArchiveDir(safe, conf):
    """
    Determine the archive directory path for a given safe based on its naming convention.

    Args:
        safe (str): safe base name
        conf (dict) configuration dictionary of cdsodatacli package

    Returns:
        gooddir (str): full path of the archive directory where the safe should be stored
    """
    logging.debug("safe: %s", safe)
    if "S1" in safe:
        firstdate = safe[17:32]
    elif "S2" in safe:
        firstdate = safe[11:26]
    elif "S3" in safe: 
        # 简单的 S3 支持，基于 S3 命名规范
        # S3A_SR_2_WAT____20170124T120058...
        splitos = safe.split("_")
        if len(splitos) > 7:
            firstdate = splitos[7]
        else:
            # fallback or error handling
            firstdate = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    else:
        # Fallback for unknown formats
        firstdate = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")

    try:
        dt = datetime.datetime.strptime(firstdate, "%Y%m%dT%H%M%S")
        year = dt.strftime("%Y")
        doy = dt.strftime("%j")
    except ValueError:
        year = "0000"
        doy = "000"

    sat = safe.split("_")[0]
    satdir = "sentinel-" + sat[2:].lower()
    
    # 尝试解析 mode/acqui
    try:
        acqui = safe.split("_")[1]
        if acqui and acqui[0] == "S":
            acqui = "SM"
    except IndexError:
        acqui = "UNKNOWN"

    # 尝试解析 level
    try:
        if "S3" in safe:
             level = safe.split("_")[2] # e.g. '2' from S3A_SR_2...
        else:
            level = safe[12:13]
    except IndexError:
        level = "0"
        
    subproddir = "L" + level
    
    # 尝试解析 subname
    if "S1" in safe:
        subname = safe[6:14]
        litlerep = sat + "_" + acqui + subname
    else:
        litlerep = safe # S3 等其他卫星可能直接用 safe 名或者简化逻辑

    gooddir = os.path.join(
        conf["archive"], satdir, subproddir, acqui, litlerep, year, doy
    )
    return gooddir


def check_safe_in_archive(safename, conf):
    """

    Check if a given safe is already present in the archive directory.

    Parameters
    ----------
    safename (str)
    conf (dict) configuration dictionary of cdsodatacli package

    Returns
    -------
        present_in_archive (bool): True -> the product is already in the archive dir. False -> not present.

    """
    present_in_archive = False
    try:
        archive_dir = WhichArchiveDir(safename, conf=conf)
    except Exception as e:
        logging.debug(f"Could not determine archive dir for {safename}: {e}")
        return False

    for uu in ["", ".zip", "replaced"]:
        arch_potential_file0 = os.path.join(archive_dir, safename)
        if uu == "":
            arch_potential_file = arch_potential_file0
        elif uu == ".zip":
            arch_potential_file = arch_potential_file0 + ".zip"
        elif uu == "replaced":
            arch_potential_file = arch_potential_file0.replace(".SAFE", ".zip")
        else:
            raise NotImplementedError
        if os.path.exists(arch_potential_file):
            present_in_archive = True
            break
    logging.debug("present_in_archive : %s", present_in_archive)
    if present_in_archive:
        logging.debug("the product is stored in : %s", arch_potential_file)
    return present_in_archive


def convert_json_opensearch_query_to_listing_safe_4_dowload(json_path) -> str:
    """

    Parameters
    ----------
    json_path str: full path of the OpenSearch file giving the meta data from the CDSE

    Returns
    -------
        output_txt str: listing with 2 columns: id,safename
    """
    logging.info("input json file: %s", json_path)
    output_txt = json_path.replace(".json", ".txt")
    with open(json_path, "r") as f:
        data = json.load(f)
    if len(data["features"]) > 0:
        df = pd.json_normalize(data["features"])
        # Fix: ensure columns exist before selecting
        if "properties.title" in df.columns and "id" in df.columns:
            sub = df[["id", "properties.title"]]
            sub.drop_duplicates()
            sub.to_csv(output_txt, header=False, index=False)
        else:
            logging.warning("JSON structure unexpected, missing 'id' or 'properties.title'")
    else:
        # Create empty file
        open(output_txt, "w").close()
            
    logging.info("output_txt : %s", output_txt)
    return output_txt
