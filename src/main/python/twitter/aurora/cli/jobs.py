import argparse
from collections import namedtuple
from twitter.aurora.cli import Noun, Verb, Context
from twitter.aurora.common.aurora_job_key import AuroraJobKey
from twitter.aurora.client.config import get_config
from twitter.aurora.client.base import (
  check_and_log_response,
  die,
  synthesize_url)
from twitter.aurora.client.api.disambiguator import LiveJobDisambiguator
from twitter.aurora.client.factory import make_client, make_client_factory
from twitter.thermos.common.options import add_binding_to

# Note: this parse_shards_into is a duplicate of a function in client v1 options.py.
# I don't want client v2 to have any dependencies on v1 code, and I don't want to modify
# client v1 code to depend on anything in client v2, thus this duplicate.
def parse_shards_into(option, opt, value, parser):
  """Parse lists of shard or shard ranges into a set().

     Examples:
       0-2
       0,1-3,5
       1,3,5
  """
  def shard_range_parser(shards):
    result = set()
    for part in shards.split(','):
      x = part.split('-')
      result.update(range(int(x[0]), int(x[-1]) + 1))
    return sorted(result)

  try:
    setattr(parser.values, option.dest, shard_range_parser(value))
  except ValueError as e:
    raise optparse.OptionValueError('Failed to parse: %s' % e)


class AuroraCommandContext(Context):
  def get_client(self):
    pass

  def get_packer(self, cluster):
    pass

  def get_api(self, cluster):
    return make_client(cluster)

  def get_job_config(self, job_key, config_file):
    select_cluster = job_key.cluster
    select_env = job_key.env
    select_role = job_key.role
    jobname = job_key.name
    return get_config(
      jobname,
      config_file,
      self.options.json,
      self.options.bindings,
      select_cluster=job_key.cluster,
      select_role=job_key.role,
      select_env=job_key.env)

  def open_page(self, url):
    import webbrowser
    webbrowser.open_new_tab(url)

  def open_job_page(self, api, config):
    self.open_page(synthesize_url(api.scheduler.scheduler().url, config.role(),
        config.environment(), config.name()))

  def handle_open(self, api):
    self.open_page(synthesize_url(api.scheduler.scheduler().url,
      self.options.jobspec.role(), self.options.jobspec.environment(), self.options.jobspec.name()))


  def exit(self, code):
    sys.exit(1)


class CreateJobCommand(Verb):
  @property
  def name(self):
    return 'create'

  @property
  def help(self):
    return 'Create a job using aurora'

  CREATE_STATES = ('PENDING','RUNNING','FINISHED')
  def setup_options_parser(self, parser):
    parser.add_argument('--bind', type=str, default=[], dest='bindings',
        action='append',
        help='Bind a thermos mustache variable name to a value. '
         'Multiple flags may be used to specify multiple values.')
    parser.add_argument('--open-browser', default=False, dest='open_browser', action='store_true',
        help='open browser to view job page after job is created')
    parser.add_argument('--json', default=False, dest='json', action='store_true',
        help='Read job configuration in json format')
    parser.add_argument('--wait_until', choices=self.CREATE_STATES,
        default='PENDING',
        help='Block the client until all the tasks have transitioned into the requested state.  Default: PENDING')
    parser.add_argument('jobspec', type=AuroraJobKey.from_path)
    parser.add_argument('config_file', type=str)

  def execute(self, context):
    try:
      config = context.get_job_config(context.options.jobspec, context.options.config_file)
    except ValueError as v:
      print('Error loading job configuration: %s' % v)
      context.exit(1)
    api = context.get_api(config.cluster())
    monitor = JobMonitor(api, config.role(), config.environment(), config.name())
    resp = api.create_job(config)
    context.check_and_log_response(resp)
    if context.options.open_browser:
      context.open_job_page(api, config)
    if context.options.wait_until == 'RUNNING':
      monitor.wait_until(monitor.running_or_finished)
    elif options.wait_until == 'FINISHED':
      monitor.wait_until(monitor.terminal)


class KillJobCommand(Verb):
  @property
  def name(self):
    return 'kill'

  def setup_options_parser(self, parser):
    parser.add_argument('--open-browser', default=False, dest='open_browser', action='store_true',
        help='open browser to view job page after job is created')
    parser.add_argument('--shards', type=str, dest='shards', default=None,
        help='A list of shard ids to act on. Can either be a comma-separated list (e.g. 0,1,2) '
            'or a range (e.g. 0-2) or any combination of the two (e.g. 0-2,5,7-9). If not set, '
            'all shards will be acted on.')
    parser.add_argument('--config', type=str, default=None, dest='config',
         help='Config file for the job, possibly containing hooks')
    parser.add_argument('jobspec', type=AuroraJobKey.from_path)
#Old shards callback=parse_shards_into,

  def execute(self, context):
    api = context.get_api(context.options.jobspec.cluster())
    resp = api.kill_job(context.options.jobspec, context.options.shards, config=context.options.config)
    context.check_and_log_response(resp)
    context.handle_open(api)


class Job(Noun):
  @property
  def name(self):
    return 'job'

  @property
  def help(self):
    return "Work with an aurora job"

  def __init__(self):
    super(Job, self).__init__()
    self.register_verb(CreateJobCommand())
    self.register_verb(KillJobCommand())


  def create_context(self, options):
    return AuroraCommandContext(options)


