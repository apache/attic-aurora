#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import json
import os
import tempfile
from io import BytesIO

import pytest
from twitter.common.contextutil import temporary_file, temporary_dir

from apache.aurora.config import AuroraConfig
from apache.aurora.config.loader import AuroraConfigLoader


BAD_MESOS_CONFIG = """
3 2 1 3 2 4 2 3
"""

JOB1 = """
HELLO_WORLD = MesosJob(
  name = 'hello_world',
  role = 'john_doe',
  cluster = 'smf1-test',
  environment = 'devel',
  task = Task(
    name = 'main',
    processes = [Process(name = 'hello_world', cmdline = 'echo {{mesos.instance}}')],
    resources = Resources(cpu = 0.1, ram = 64 * 1048576, disk = 64 * 1048576),
  )
)
"""

JOB2 = """
OTHERJOB = MesosJob(
  name = 'otherjob',
  role = 'john_doe',
  cluster = 'smf1-test',
  environment = 'devel',
  task = Task(
    name = 'main',
    processes = [Process(name = 'otherthing', cmdline = 'echo {{mesos.instance}}')],
    resources = Resources(cpu = 0.1, ram = 64 * 1048576, disk = 64 * 1048576),
  )
)
"""

MESOS_CONFIG = JOB1 + """
jobs = [HELLO_WORLD]
"""

MESOS_CONFIG_MULTI = JOB1 + JOB2 + """
jobs = [HELLO_WORLD, OTHERJOB]
"""

MESOS_CONFIG_WITH_INCLUDE_TEMPLATE = JOB2 + """
include("./%s")
jobs = [HELLO_WORLD, OTHERJOB]
"""


def test_enoent():
  nonexistent_file = tempfile.mktemp()
  with pytest.raises(AuroraConfigLoader.NotFound):
    AuroraConfigLoader.load(nonexistent_file)


def test_bad_config():
  with pytest.raises(AuroraConfigLoader.InvalidConfigError):
    AuroraConfigLoader.load(BytesIO(BAD_MESOS_CONFIG))


def test_filter_schema():
  env = AuroraConfigLoader.load(BytesIO(MESOS_CONFIG))
  job_dict = env['jobs'][0].get()
  job_dict['unknown_attribute'] = 'foo bar baz'
  job_json_string = json.dumps(job_dict)
  with pytest.raises(AttributeError):
    AuroraConfigLoader.loads_json(job_json_string)


def test_empty_config():
  AuroraConfigLoader.load(BytesIO())


def test_load_json_single():
  env = AuroraConfigLoader.load(BytesIO(MESOS_CONFIG))
  job = env['jobs'][0]
  new_job = AuroraConfigLoader.loads_json(json.dumps(job.get()))['jobs'][0]
  assert new_job == job


def test_load_json_memoized():
  """
  This test reads 2 jobs from the MESOS_CONFIG_MULTI string.
  It then tests memoization by:
  1. write job[0] as json to tmp/config.json
  2. load_json(tmp/config.json, is_memozied=True) to prime the cache
  3. overwrite tmp/config.json with job[1] as json.
  4. call load_json(tmp/config.json, is_memoized=True) and verify that job[0] is read
     (matching the cached version of config.json, not the current version)
  5. call load_json(tmp/config.json, is_memoized=False) and verify witout memoizaition
     the new content is read (job[1])

  """
  env = AuroraConfigLoader.load(BytesIO(MESOS_CONFIG_MULTI))
  jobs = env['jobs']

  with temporary_dir() as d:
    with open(os.path.join(d, 'config.json'), 'w+') as fp:
      # write job[0] to config.json
      fp.write(json.dumps(jobs[0].get()))
      fp.close()
      # read job[0] from config.json and set the cached value
      new_job = AuroraConfigLoader.load_json(fp.name, is_memoized=True)['jobs'][0]
      assert new_job == jobs[0]

    with open(os.path.join(d, 'config.json'), 'w+') as fp:
      # overwrite config.json with job[1]
      fp.write(json.dumps(jobs[1].get()))
      fp.close()
      after_overwrite = AuroraConfigLoader.load_json(fp.name, is_memoized=True)['jobs'][0]
      # verify that value we loaded is the cached value(job[0]) when is_memoized=True
      assert after_overwrite == jobs[0]
      after_overwrite_no_memozied = AuroraConfigLoader.load_json(
        fp.name, is_memoized=False)['jobs'][0]
      # without memoization, verify that value we load is the uncached value(job[1])
      assert after_overwrite_no_memozied == jobs[1]


def test_load_json_multi():
  env = AuroraConfigLoader.load(BytesIO(MESOS_CONFIG_MULTI))
  jobs = env['jobs']
  json_env = AuroraConfigLoader.loads_json(json.dumps({'jobs': [job.get() for job in jobs]}))
  json_jobs = json_env['jobs']
  assert jobs == json_jobs


def test_load():
  with temporary_file() as fp:
    fp.write(MESOS_CONFIG)
    fp.flush()
    fp.seek(0)

    for config in (fp.name, fp):
      env = AuroraConfigLoader.load(config)
      assert 'jobs' in env and len(env['jobs']) == 1
      hello_world = env['jobs'][0]
      assert hello_world.name().get() == 'hello_world'


def test_load_with_includes():
  with temporary_dir() as tmp_dir:
    f1_name = 'f1.aurora'
    f2_name = 'f2.aurora'
    with open(os.path.join(tmp_dir, f1_name), 'w+') as f1:
      f1.write(MESOS_CONFIG)
      f1.flush()
      f1.seek(0)
      with open(os.path.join(tmp_dir, f2_name), 'w+') as f2:
        f2.write(MESOS_CONFIG_WITH_INCLUDE_TEMPLATE % f1_name)
        f2.flush()
        f2.seek(0)

        env = AuroraConfigLoader.load(f2.name, is_memoized=True)
        assert 'jobs' in env and len(env['jobs']) == 2
        hello_world = env['jobs'][0]
        assert hello_world.name().get() == 'hello_world'
        other_job = env['jobs'][1]
        assert other_job.name().get() == 'otherjob'


def test_memoized_load():
  with temporary_dir() as d:
    with open(os.path.join(d, 'config.aurora'), 'w+') as fp:
      fp.write(MESOS_CONFIG)
      fp.flush()
      fp.seek(0)

      for config in (fp.name, fp):
        env = AuroraConfigLoader.load(config, is_memoized=True)
        assert 'jobs' in env and len(env['jobs']) == 1
        hello_world = env['jobs'][0]
        assert hello_world.name().get() == 'hello_world'

    with open(os.path.join(d, 'config.aurora'), 'w+') as fp:
      fp.write(MESOS_CONFIG_MULTI)
      fp.flush()
      fp.seek(0)

      for config in (fp.name, fp):
        # Verfiy Cached Content is from initial write/read, (1 job)
        env = AuroraConfigLoader.load(config, is_memoized=True)
        assert 'jobs' in env and len(env['jobs']) == 1
        hello_world = env['jobs'][0]
        assert hello_world.name().get() == 'hello_world'

        # Verfiy uncached content is from second write, (2 jobs)
        env_no_cache = AuroraConfigLoader.load(config, is_memoized=False)
        assert 'jobs' in env_no_cache and len(env_no_cache['jobs']) == 2
        other_job = env_no_cache['jobs'][1]
        assert other_job.name().get() == 'otherjob'


def test_pick():
  env = AuroraConfigLoader.load(BytesIO(MESOS_CONFIG))

  hello_world = env['jobs'][0]
  assert AuroraConfig.pick(env, 'hello_world', None) == hello_world

  env['jobs'][0] = env['jobs'][0](name='something_{{else}}')
  assert str(AuroraConfig.pick(env, 'something_else', [{'else': 'else'}]).name()) == (
      'something_else')
