import argparse
from collections import namedtuple

from twitter.aurora.client.api.disambiguator import LiveJobDisambiguator
from twitter.aurora.client.api.job_monitor import JobMonitor
from twitter.aurora.client.cli import (
    EXIT_COMMAND_FAILURE,
    EXIT_INVALID_COMMAND,
    EXIT_INVALID_CONFIGURATION,
    EXIT_INVALID_PARAMETER,
    EXIT_NETWORK_ERROR,
    EXIT_OK,
    EXIT_PERMISSION_VIOLATION,
    EXIT_TIMEOUT,
    EXIT_UNKNOWN_ERROR,
    Noun,
    Verb
)
from twitter.aurora.client.cli.context import AuroraCommandContext
from twitter.aurora.common.aurora_job_key import AuroraJobKey
from twitter.aurora.client.config import get_config
from twitter.aurora.client.factory import make_client

from pystachio.config import Config

# Note: this parse_shards_into is a near duplicate of a function in client v1 options.py.
# I don't want client v2 to have any dependencies on v1 code, and I don't want to modify
# client v1 code to depend on anything in client v2, thus this duplicate.
def parse_shards_into(values):
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

  result = []
  for v in values:
    result.append(shard_range_parser(value))
  return result


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
        help=('Block the client until all the tasks have transitioned into the requested state. '
                        'Default: PENDING'))
    parser.add_argument('jobspec', type=AuroraJobKey.from_path)
    parser.add_argument('config_file', type=str)

  def execute(self, context):
    try:
      config = context.get_job_config(context.options.jobspec, context.options.config_file)
    except Config.InvalidConfigError as e:
      print('Error loading job configuration: %s' % e)
      context.exit(EXIT_INVALID_CONFIGURATION)
    api = context.get_api(config.cluster())
    monitor = JobMonitor(api, config.role(), config.environment(), config.name())
    resp = api.create_job(config)
    context.check_and_log_response(resp)
    if context.options.open_browser:
      context.open_job_page(api, config)
    if context.options.wait_until == 'RUNNING':
      monitor.wait_until(monitor.running_or_finished)
    elif context.options.wait_until == 'FINISHED':
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

  def execute(self, context):
    # Old shards callback=parse_shards_into,
    shards = parse_shards_into(context.options.shards)
    api = context.get_api(context.options.jobspec.cluster())
    resp = api.kill_job(context.options.jobspec, shards, config=context.options.config)
    context.check_and_log_response(resp)
    context.handle_open(api)


class Job(Noun):
  @property
  def name(self):
    return 'job'

  @property
  def help(self):
    return "Work with an aurora job"

  @classmethod
  def context_type(cls):
    return

  def __init__(self, context_type=AuroraCommandContext):
    super(Job, self).__init__()
    self.register_verb(CreateJobCommand())
    self.register_verb(KillJobCommand())
