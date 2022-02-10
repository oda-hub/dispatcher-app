import os.path
import nbformat as nbf
import time
import shutil

from git import Repo
from urllib.parse import urlparse

from ..app_logging import app_logging
from .exceptions import RequestNotUnderstood

logger = app_logging.getLogger('renku_helper')


def push_api_code(api_code,
                  job_id,
                  renku_repository_url,
                  renku_gitlab_token_name,
                  renku_gitlab_token,
                  sentry_client=None):
    error_message = 'Error while {step}'
    try:
        step = 'cloning repository'
        repo = clone_renku_repo(renku_repository_url,
                                renku_gitlab_token_name=renku_gitlab_token_name,
                                renku_gitlab_token=renku_gitlab_token)
        step = 'assigning branch name'
        branch_name = get_branch_name(job_id=job_id)

        step = f'checkout branch {branch_name}'
        repo = checkout_branch_renku_repo(repo, branch_name)

        step = f'creating new notebook with the api code'
        new_file_path = create_new_notebook_with_code(repo, api_code, job_id)

        step = f'committing and pushing notebook {new_file_path} to the repository'
        commit_and_push_file(repo, new_file_path)

    except Exception as e:
        error_message = error_message.format(step=step)

        if sentry_client is not None:
            sentry_client.capture('raven.events.Message',
                                  message=f'{error_message}\n{e}')
        raise RequestNotUnderstood(error_message)
    finally:
        logger.info("==> removing repository folder, since it is no longer necessary")
        remove_repository(repo)
    # TODO to actually return the renkulab url of the newly created branch
    return repo.remotes.origin.url


def get_repo_name(repository_url):
    repo_name = repository_url.split('/')[-1]
    if repo_name.endswith('.git'):
        repo_name = repo_name[0:-4]

    return repo_name


def clone_renku_repo(renku_repository_url, repo_dir=None, renku_gitlab_token_name=None, renku_gitlab_token=None):
    if repo_dir is None:
        repo_dir = get_repo_name(renku_repository_url)

    url_parsed = urlparse(renku_repository_url)

    if renku_gitlab_token_name is not None and renku_gitlab_token is not None:
        url_parsed = url_parsed._replace(netloc=f'{renku_gitlab_token_name}:{renku_gitlab_token}@{url_parsed.hostname}')

    repo = Repo.clone_from(url_parsed.geturl(), repo_dir, branch='master')

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
        origin = repo.remote(name="origin")
        # TODO make it work with methods from GitPython
        # e.g. push_info = origin.push(refspec='origin:' + str(repo.head.ref))
        push_info = repo.git.push("--set-upstream", repo.remote().name, str(repo.head.ref))
        logger.info("push operation complete")
    except Exception as e:
        logger.warning(f"something happened while pushing the the file {file_path}, {e}")
        raise e


def remove_repository(repo):
    if os.path.exists(repo.working_dir):
        shutil.rmtree(repo.working_dir)

