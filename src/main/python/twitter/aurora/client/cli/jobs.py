from twitter.aurora.client.api.job_monitor import JobMonitor
from twitter.aurora.client.cli import (
    EXIT_INVALID_CONFIGURATION,
    Noun,
    Verb
)
from twitter.aurora.client.cli.context import AuroraCommandContext
from twitter.aurora.common.aurora_job_key import AuroraJobKey

from pystachio.config import Config


def parse_shards(shards):
  """Parse lists of shard or shard ranges into a set().

     Examples:
       0-2
       0,1-3,5
       1,3,5
  """
  print('SHARDS="%s"' % shards)
  if shards is None or shards == '':
    return None
  result = set()
  for part in shards.split(','):
    x = part.split('-')
    result.update(range(int(x[0]), int(x[-1]) + 1))
  return sorted(result)


class CreateJobCommand(Verb):
  @property
  def name(self):
    return 'create'

  @property
  def help(self):
    return 'Create a job using aurora'

  CREATE_STATES = ('PENDING', 'RUNNING', 'FINISHED')

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
      raise context.CommandError(EXIT_INVALID_CONFIGURATION,
          'Error loading job configuration: %s' % e)
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
    shards = parse_shards(context.options.shards)
    api = context.get_api(context.options.jobspec.cluster)
    resp = api.kill_job(context.options.jobspec, shards)
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
  def create_context(cls):
    return AuroraCommandContext()

  def __init__(self):
    super(Job, self).__init__()
    self.register_verb(CreateJobCommand())
    self.register_verb(KillJobCommand())
