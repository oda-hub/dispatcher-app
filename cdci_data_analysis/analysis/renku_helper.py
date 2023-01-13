import os.path
import re
import tempfile
import traceback
import nbformat as nbf
import shutil
import giturlparse
import copy

from git import Repo, Actor
from configparser import ConfigParser

from ..app_logging import app_logging
from .exceptions import RequestNotUnderstood
from .email_helper import generate_products_url_from_par_dict
from .hash import make_hash

logger = app_logging.getLogger('renku_helper')


def push_api_code(api_code,
                  job_id,
                  token,
                  renku_gitlab_repository_url,
                  renku_gitlab_ssh_key_path,
                  renku_base_project_url,
                  user_name=None,
                  user_email=None,
                  products_url=None,
                  request_dict=None):
    error_message = 'Error while {step}'
    repo = None
    try:
        step = 'cloning repository'
        logger.info(step)
        repo = clone_renku_repo(renku_gitlab_repository_url,
                                renku_gitlab_ssh_key_path=renku_gitlab_ssh_key_path)

        step = f'removing token from the api_code'
        logger.info(step)
        token_pattern = r"[\'\"]token[\'\"]:\s*?[\'\"].*?[\'\"]"
        api_code = re.sub(token_pattern, '"token": os.environ[\'ODA_TOKEN\'],', api_code, flags=re.DOTALL)
        api_code = "import os\n\n" + api_code

        step = 'creating new notebook with the api code'
        logger.info(step)
        file_name = 'api_code.ipynb'
        nb_obj = create_new_notebook_with_code(api_code)

        step = 'generating hash of the notebook content'
        logger.info(step)
        notebook_hash = generate_nb_hash(nb_obj)

        step = 'creating renku ini config file'
        logger.info(step)
        config_ini_obj = create_renku_ini_config_obj(repo, file_name)

        step = 'generating hash of the config content'
        logger.info(step)
        config_ini_hash = generate_ini_file_hash(config_ini_obj)

        step = 'assigning branch name, using the job_id and the notebook hash'
        logger.info(step)
        branch_name = get_branch_name(job_id=job_id, notebook_hash=notebook_hash, renku_ini_hash=config_ini_hash)

        step = 'check branch existence, using the job_id and the notebook hash'
        logger.info(step)
        branch_existing = check_job_id_branch_is_present(repo, job_id=job_id, notebook_hash=notebook_hash, config_ini_hash=config_ini_hash)

        step = f'checkout branch {branch_name}'
        if branch_existing:
            step += ', since the branch already exists so we perform a git pull'
        else:
            step += ', but we don\'t perform any git pull since the branch does not exist'
        logger.info(step)
        repo = checkout_branch_renku_repo(repo, branch_name, pull=branch_existing)

        if not branch_existing:
            step = 'updating renku ini file for the starting notebook, and push the update'
            logger.info(step)
            update_and_commit_default_url_renku_ini(repo, config_ini_obj, user_name=user_name, user_email=user_email)

            step = 'writing notebook file'
            logger.info(step)
            new_file_path = write_notebook_file(repo, nb_obj, file_name)

            step = 'committing and pushing the api code to the renku repository'
            logger.info(step)
            commit_info = commit_and_push_notebook_file(repo, new_file_path, user_name=user_name, user_email=user_email, products_url=products_url, request_dict=request_dict)

        else:
            commit_info = repo.head.commit

        step = 'generating a valid url to start a new session on the new branch'
        logger.info(step)
        renku_session_url = generate_renku_session_url(repo,
                                                       renku_base_project_url=renku_base_project_url,
                                                       branch_name=branch_name,
                                                       commit=commit_info.hexsha,
                                                       # notebook_name=file_name,
                                                       token=token)

    except Exception as e:
        error_message = error_message.format(step=step)
        logger.warning(f"something happened while pushing the api_code: {step}, {e}")
        traceback.print_exc()

        raise RequestNotUnderstood(error_message)
    finally:
        logger.info("==> removing repository folder, since it is no longer necessary")
        remove_repository(repo, renku_gitlab_repository_url)

    return renku_session_url


def generate_renku_session_url(repo, renku_base_project_url, branch_name, commit=None, notebook_name=None, token=None):
    original_url = repo.remotes.origin.url
    repo_path = get_repo_path(original_url)
    renku_project_url = f'{renku_base_project_url}/{repo_path}'
    output_url = f'{renku_project_url}/sessions/new?autostart=1&branch={branch_name}'
    if commit is not None:
        output_url += f'&commit={commit}'
    if notebook_name is not None:
        output_url += f'&notebook={notebook_name}'
    if token is not None:
        output_url += f'&env[ODA_TOKEN]={token}'
    return output_url
    

def get_repo_path(repository_url):
    git_parsed_url = giturlparse.parse(repository_url)
    if git_parsed_url.valid:
        repo_path = git_parsed_url.pathname
        match = re.search(".git$", repo_path)
        if match:
            repo_path = repo_path[0:-4]
        return repo_path
    else:
        raise Exception(f"{repository_url} is not in a valid repository url format of, please check it and try again")


def get_repo_name(repository_url):
    git_parsed_url = giturlparse.parse(repository_url)
    if git_parsed_url.valid:
        return git_parsed_url.name
    else:
        raise Exception(f"{repository_url} is not in a valid repository url format of, please check it and try again")


def get_repo_local_path(repository_url):
    return tempfile.mkdtemp(prefix=get_repo_name(repository_url))    


def clone_renku_repo(renku_repository_url, repo_dir=None, renku_gitlab_ssh_key_path=None):
    logger.info('clone_renku_repo with renku_repository_url=%s, repo_dir=%s, renku_gitlab_ssh_key_file=%s', renku_repository_url, repo_dir, renku_gitlab_ssh_key_path)

    if repo_dir is None:
        repo_dir = get_repo_local_path(renku_repository_url)
        logger.info('constructing repo_dir=%s', repo_dir)

    # TODO or store known hosts on build/boot
    git_ssh_cmd = f'ssh -i {renku_gitlab_ssh_key_path} -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'

    repo = Repo.clone_from(renku_repository_url, repo_dir, branch='master', env=dict(GIT_SSH_COMMAND=git_ssh_cmd))

    logger.info(f'repository {renku_repository_url} successfully cloned')

    return repo


def get_list_remote_branches_repo(repo):

    list_branches = repo.git.branch("-ar", "--format=%(refname:short)").split("\n")

    return list_branches


def check_job_id_branch_is_present(repo, job_id, notebook_hash, config_ini_hash):
    list_branches = get_list_remote_branches_repo(repo)

    r = re.compile(f"^(?!renku/autosave/).*_{job_id}_{notebook_hash}_{config_ini_hash}")
    filtered_list = list(filter(r.match, list_branches))

    return len(filtered_list) == 1


def get_branch_name(job_id=None, notebook_hash=None, renku_ini_hash=None):
    branch_name = 'mmoda_request'

    if job_id is not None:
        branch_name += f'_{job_id}'

    if notebook_hash is not None:
        branch_name += f'_{notebook_hash}'

    if renku_ini_hash is not None:
        branch_name += f'_{renku_ini_hash}'

    return branch_name


def checkout_branch_renku_repo(repo, branch_name, pull=False):
    repo.git.checkout('-b', branch_name)
    if pull:
        repo.git.pull("--set-upstream", repo.remote().name, str(repo.head.ref))
        logger.info("pull operation complete")

    return repo


def generate_notebook_filename(job_id):
    return "_".join(["api_code", job_id]) + '.ipynb'


def write_notebook_file(repo, nb, file_name):
    repo_dir = repo.working_dir
    file_path = os.path.join(repo_dir, file_name)
    nbf.write(nb, file_path)

    return file_path


def add_commit_push(repo, commit_msg, files_path_to_add, user_name=None, user_email=None):
    if isinstance(files_path_to_add, list):
        for path in files_path_to_add:
            repo.index.add(path)
    else:
        repo.index.add(files_path_to_add)

    author = None

    if user_name is not None:
        author = Actor(user_name, user_email)

    commit_info = repo.index.commit(commit_msg, author=author)
    repo.remote(name="origin")
    # TODO make it work with methods from GitPython
    # e.g. push_info = origin.push(refspec='origin:' + str(repo.head.ref))
    repo.git.push("--set-upstream", repo.remote().name, str(repo.head.ref))

    return commit_info


def update_and_commit_default_url_renku_ini(repo, config_obj, user_name=None, user_email=None):
    repo_dir = repo.working_dir

    renku_ini_path = os.path.join(repo_dir, '.renku', 'renku.ini')

    with open(renku_ini_path, 'w') as renku_ini_file:
        config_obj.write(renku_ini_file)

    commit_msg = "Update Renku config file with starting notebook"
    add_commit_push(repo, commit_msg, renku_ini_path, user_name, user_email)
    logger.info("renku config push operation complete")

    return renku_ini_path


def generate_ini_file_hash(config_ini_obj):
    try:
        ini_config_dict = { s:dict(config_ini_obj.items(s)) for s in config_ini_obj.sections() }
        ini_hash = make_hash(ini_config_dict)
    except:
        logger.error(f'Unable to generate a hash of the ini config file: {ini_config_dict}')
        raise Exception(f'Unable to generate a hash of the ini config file: {ini_config_dict}')

    return ini_hash


def generate_nb_hash(nb_obj):
    copied_notebook_obj = copy.deepcopy(nb_obj)

    try:
        for cell in copied_notebook_obj['cells']:
            cell.pop('id', None)
        notebook_hash = make_hash(copied_notebook_obj)
    except:
        logger.error(f'Unable to generate a hash of the notebook object: {copied_notebook_obj}')
        raise Exception(f'Unable to generate a hash of the notebook object: {copied_notebook_obj}')


    return notebook_hash


def create_renku_ini_config_obj(repo, default_url_file_name):
    repo_dir = repo.working_dir

    renku_ini_path = os.path.join(repo_dir, '.renku', 'renku.ini')

    renku_config = ConfigParser()
    renku_config.read(renku_ini_path)
    renku_config['renku "interactive"']['default_url'] = f'/lab/tree/{default_url_file_name}'

    return renku_config

def create_new_notebook_with_code(api_code):
    nb = nbf.v4.new_notebook()

    text = "# Notebook automatically generated from MMODA"

    nb['cells'] = [nbf.v4.new_markdown_cell(text),
                   nbf.v4.new_code_cell(api_code)]

    nb['metadata']['kernelspec'] = {
        "display_name": "Python 3",
        "language": "python",
        "name": "python3"
    }

    return nb


def generate_commit_request_url(products_url, params_dic, use_scws=None):
    # generate the url for the commit message
    # this is a "default" value for use_scws
    params_dic['use_scws'] = 'no'
    if 'scw_list' in params_dic:
        # for the frontend
        params_dic['use_scws'] = 'form_list'

    request_url = generate_products_url_from_par_dict(products_url, params_dic)
    return request_url


def commit_and_push_notebook_file(repo, file_path, user_name=None, user_email=None, products_url=None, request_dict=None):
    commit_msg = "Stored API code of MMODA request"
    if user_name is not None:
        commit_msg += f" by {user_name}"

    if request_dict is not None:
        if 'product_type' in request_dict:
            commit_msg += f" for a {request_dict['product_type']}"
        if 'instrument' in request_dict:
            commit_msg += f" from the instrument {request_dict['instrument']}"
        request_url = generate_commit_request_url(products_url, request_dict)
        commit_msg += (f"\nthe original request was generated via {request_url}\n"
                       "to retrieve the result please follow the link")

    commit_info = add_commit_push(repo, commit_msg, file_path, user_name, user_email)
    logger.info("notebook commit push operation complete")

    return commit_info


def remove_repository(repo, renku_repository_url):
    repo_working_dir_path = None
    if repo is not None:
        repo_working_dir_path = repo.working_dir

    if repo_working_dir_path is not None and os.path.exists(repo_working_dir_path):
        logger.info('removing repo_working_dir_path=%s created for renku_repository_url=%s', repo_working_dir_path, renku_repository_url)
        try:
            shutil.rmtree(repo_working_dir_path)
        except OSError as e:
            logger.error('unable to remove repo directory repo_working_dir_path=%s !')

