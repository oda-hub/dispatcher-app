import os.path
import nbformat as nbf
import time

from git import Repo

from ..app_logging import app_logging

logger = app_logging.getLogger('renku_helper')


def get_repo_name(repository_url):
    repo_name = repository_url.split('/')[-1]
    if repo_name.endswith('.git'):
        repo_name = repo_name[0:-4]

    return repo_name


def clone_renku_repo(renku_repository_url, repo_dir=None):
    if repo_dir is None:
        repo_dir = get_repo_name(renku_repository_url)

    repo = Repo.clone_from(renku_repository_url, repo_dir, branch='master')

    logger.info(f'repository {renku_repository_url} successfully cloned')

    return repo


def get_branch_name(job_id=None, session_id=None):
    branch_name = 'mmoda_request'

    if job_id is not None:
        branch_name += f'_{job_id}'

    if session_id is not None:
        branch_name += f'_{session_id}'

    return branch_name


def checkout_branch_renku_repo(repo, branch_name):
    repo.git.checkout('-b', branch_name)

    return repo


def create_new_notebook_with_code(repo, code, job_id, file_name=None):
    repo_dir = repo.working_dir

    if file_name is None:
        file_name = "_".join(["code", job_id, str(time.time())]) + '.ipynb'

    file_path = os.path.join(repo_dir, file_name)

    nb = nbf.v4.new_notebook()

    text = """\
    # Notebook automatically generated from MMODA"""

    nb['cells'] = [nbf.v4.new_markdown_cell(text),
                   nbf.v4.new_code_cell(code)]

    nbf.write(nb, file_path)

    return file_path


def commit_and_push_file(repo, file_path):
    try:
        add_info = repo.index.add(file_path)
        commit_info = repo.index.commit("commit code from MMODA")
        origin = repo.remote(name='origin')
        push_info = origin.push(refspec='origin:' + str(repo.head.ref))
    except Exception as e:
        logger.warning(f"something happened while pushing the the file {file_path}, {e}")
        raise e

