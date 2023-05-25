import os, sys
import yaml
import signal
import glob
import json

def allowed_file(filename, ALLOWED_EXTENSIONS):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def load_yaml(file):
    etc_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etc')
    file_path = os.path.join(etc_path, file)
    if not os.path.isfile(file_path):
        print("File not found at location {}".format(file_path))
        sys.exit(1)
    with open(file_path) as cfile:
        config = yaml.load(cfile, Loader=yaml.FullLoader)
    return config


def save_config_yaml(config_dict):
    etc_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etc')
    config_file = "config.yaml"
    with open(os.path.join(etc_path, config_file), 'w') as file:
        documents = yaml.dump(config_dict, file)


def save_aug_yaml(aug_dict):
    etc_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etc')
    aug_file = "aug_cfg.yaml"
    with open(os.path.join(etc_path, aug_file), 'w') as file:
        yaml.dump(aug_dict, file)


def add_prefix(data_path):
    file_name = data_path.split('/')[-1].split('.')[0]
    file_ext = data_path.split('/')[-1].split('.')[-1]
    new_input_name = f'{file_name}_AUG.{file_ext}'
    # print(new_input_name)
    return new_input_name


def check_file(file):
    if os.path.isfile(file):
        return True
    else:
        return False


def save_pid(pid, file):
    etc_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etc')
    pid_loc = os.path.join(etc_path, file)
    with open(pid_loc, 'w') as f:
        f.write(str(pid))


def clean_up_pid(file):
    etc_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etc')
    pid_loc = os.path.join(etc_path, file)
    if not os.path.isfile(pid_loc):
        return -1
    os.remove(pid_loc)
    return 0


def kill_pid(pid):
    os.kill(pid, signal.SIGTERM)


def get_pid_from_file(file, check=False):
    etc_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etc')
    pid_loc = os.path.join(etc_path, file)
    if not os.path.isfile(pid_loc):
        return -1
    else:
        with open(pid_loc, 'r') as f:
            pid = f.readline()
        if check:
            if check_pid(int(pid)):
                return 1
            else:
                return 0
        return int(pid)


def check_pid(pid):
    """ Check For the existence of a unix pid. """
    try:
        os.kill(int(pid), 0)
    except OSError:
        return False
    else:
        return True


def get_list_workers():
    etc_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etc')
    g_dir = "{}/*.pid".format(etc_path)
    list_workers = []
    for name in glob.glob(g_dir):
        list_workers.append(name)
    return list_workers


def get_list_data_files(data_folder):
    g_dir = "{}/*.csv".format(data_folder)
    list_data_files = []
    for name in glob.glob(g_dir):
        list_data_files.append(name.split('/')[-1])
    return list_data_files


def delete_pid_file(file):
    etc_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etc')
    pid_loc = os.path.join(etc_path, file)
    if os.path.isfile(pid_loc):
        os.remove(pid_loc)


def load_schema(file):
    etc_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etc')
    file_path = os.path.join(etc_path, file)
    with open(file_path) as cfile:
        schema = json.load(cfile)
    return schema


def check_and_rename(file):
    if os.path.isfile(file):
        os.rename(file, file + ".old")

def percentage(percent, whole):
  return (percent * whole) / 100.0