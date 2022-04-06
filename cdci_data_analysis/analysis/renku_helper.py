import os.path
import re
import tempfile
import traceback
import time

import nbformat as nbf
import shutil

from git import Repo
import giturlparse

from ..app_logging import app_logging
from .exceptions import RequestNotUnderstood

logger = app_logging.getLogger('renku_helper')


def push_api_code(api_code,
                  job_id,
                  renku_gitlab_repository_url,
                  renku_gitlab_ssh_key_path,
                  renku_base_project_url,
                  sentry_client=None,
                  user=None):
    error_message = 'Error while {step}'
    repo = None
    try:
        step = 'cloning repository'
        repo = clone_renku_repo(renku_gitlab_repository_url,
                                renku_gitlab_ssh_key_path=renku_gitlab_ssh_key_path)
        step = 'assigning branch name'
        branch_name = get_branch_name(job_id=job_id)

        step = f'checkout branch {branch_name}'
        repo = checkout_branch_renku_repo(repo, branch_name)

        step = f'removing token from the api_code'
        token_pattern = r"(\'|\")token(\'|\"):.\s?(\'|\").*?(\'|\"),?"
        api_code = re.sub(token_pattern, '# "token": "getpass.getpass()",', api_code, flags=re.DOTALL)

        step = f'creating new notebook with the api code'
        new_file_path = create_new_notebook_with_code(repo, api_code, job_id)

        step = f'committing and pushing the api code to the renku repository'
        commit_and_push_file(repo, new_file_path)

        step = f'generating a valid url to start a new session on the new branch'
        renku_session_url = generate_renku_session_url(repo,
                                                       renku_base_project_url=renku_base_project_url,
                                                       branch_name=branch_name)

    except Exception as e:
        error_message = error_message.format(step=step)

        traceback.print_exc()

        if sentry_client is not None:
            sentry_client.capture('raven.events.Message',
                                  message=f'{error_message}\n{e}')
        raise RequestNotUnderstood(error_message)
    finally:
        logger.info("==> removing repository folder, since it is no longer necessary")
        remove_repository(repo, renku_gitlab_repository_url)

    return renku_session_url


def generate_renku_session_url(repo, renku_base_project_url, branch_name):
    original_url = repo.remotes.origin.url
    repo_path = get_repo_path(original_url)
    renku_project_url = f'{renku_base_project_url}/{repo_path}'
    return f"{renku_project_url}/sessions/new?autostart=1&branch={branch_name}"
    

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

    list_branches = repo.git.branch("-a", "--format=%(refname:short)").split("\n")

    return list_branches


def check_job_id_branch_is_present(repo, job_id):
    list_branches = get_list_remote_branches_repo(repo)

    r = re.compile(f".*_{job_id}")
    filtered_list = list(filter(r.match, list_branches))

    return len(filtered_list) == 1


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


def create_new_notebook_with_code(repo, api_code, job_id, file_name=None):
    repo_dir = repo.working_dir

    if file_name is None:
        file_name = "_".join(["api_code", job_id]) + '.ipynb'

    file_path = os.path.join(repo_dir, file_name)

    nb = nbf.v4.new_notebook()

    text = "# Notebook automatically generated from MMODA"

    api_code = "import getpass\n\n" + api_code

    nb['cells'] = [nbf.v4.new_markdown_cell(text),
                   nbf.v4.new_code_cell(api_code)]

    nbf.write(nb, file_path)

    return file_path


def commit_and_push_file(repo, file_path, user=None):
    try:
        add_info = repo.index.add(file_path)
        commit_msg = "commit code from MMODA"
        if user is not None:
            commit_msg += " " + user
        commit_info = repo.index.commit(commit_msg)
        origin = repo.remote(name="origin")
        # TODO make it work with methods from GitPython
        # e.g. push_info = origin.push(refspec='origin:' + str(repo.head.ref))
        push_info = repo.git.push("--set-upstream", repo.remote().name, str(repo.head.ref), "--force")
        logger.info("push operation complete")
    except Exception as e:
        logger.warning(f"something happened while pushing the the file {file_path}, {e}")
        raise e


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

