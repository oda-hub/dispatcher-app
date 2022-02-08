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
    # repo.heads.past_branch.checkout()

    return repo


def create_renku_branch():
    pass
