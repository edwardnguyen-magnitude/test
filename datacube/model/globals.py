import os
from pathlib import Path

from cube import TemplateContext
from cube_dbt import Dbt

template = TemplateContext()

# Derive project name from DBT_PROJECT_DIR (e.g. /opt/dagster/projects/reg -> reg)
_project_name = Path(os.environ.get('DBT_PROJECT_DIR', 'projects/reg')).name
_domain_src = Path(f'/cube/conf/projects/{_project_name}/cube')
_domain_dst = Path('/cube/conf/model/domain')
if _domain_src.exists() and not _domain_dst.exists():
    _domain_dst.symlink_to(_domain_src)

MANIFEST_PATH = '/cube/conf/dbt_target/manifest.json'
dbt_all = Dbt.from_file(MANIFEST_PATH)
dbt_marts = Dbt.from_file(MANIFEST_PATH).filter(paths=['marts/'])


@template.function('dbt_models')
def dbt_models():
    return dbt_marts.models


@template.function('dbt_model')
def dbt_model(name):
    return dbt_all.model(name)


@template.function('cube_name')
def cube_name(dbt_name):
    return dbt_name.removeprefix('mart_')
