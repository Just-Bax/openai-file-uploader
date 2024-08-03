# module-template

Template repository to create new modules

For Python code follow [style guidelines](https://peps.python.org/pep-0008/)

For configuration installation samples, review files in the `installation` directory

Make sure to rename `.integration_` file to `.integration` for module to be available in the list in UI


## Mirror repository configuration
The provided GitHub Workflow `mirror-repository.yml` automates the synchronization of repositories in different GitHub orgs without using forking.
Steps to setup sync:
1. manually create a mirror of the repository in another organization (assuming the source org is `ov-integrations` and the target is `VizionHub`):
```shell
git clone --mirror https://github.com/ov-integrations/<repo_name>.git
cd <repo_name>
git push --mirror https://github.com/VizionHub/<repo_name>
```
2. generate two PATs to be used for workflow:
   * the first one should have `Contents - Read` privs for the source repository (i.e. in the 'ov-integrations' org). Store token in the `OV_SOURCE_REPOSITORY_PAT_READ_REPO` action secret of the mirror repository created at step `1`.
   * the second token must have `Contents - Read and write` and `Workflows - Read and write` for the target reposiory (i.e. in the `VizionHub` org). Store token in the `OV_MIRROR_REPOSITORY_PAT_WRITE_REPO` action secret of the mirror repository created at step `1`.
3. Set value for `OV_SOURCE_ORGANIZATION` action variable with the name of the source org, i.e. `ov-integrations`

> Values of existing `VizionHub Mirror WF` and `VizionHub Mirror WF - write` PATs of the `ov-automation` account may be used for `OV_SOURCE_REPOSITORY_PAT_READ_REPO` and `OV_MIRROR_REPOSITORY_PAT_WRITE_REPO` accordingly.

   
