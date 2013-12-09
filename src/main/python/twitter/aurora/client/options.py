import optparse

from twitter.aurora.common.aurora_job_key import AuroraJobKey
from twitter.thermos.common.options import add_binding_to


__all__ = (
  'CLUSTER_CONFIG_OPTION',
  'CLUSTER_INVOKE_OPTION',
  'ENVIRONMENT_BIND_OPTION',
  'ENV_CONFIG_OPTION',
  'EXECUTOR_SANDBOX_OPTION',
  'FROM_JOBKEY_OPTION',
  'HEALTH_CHECK_INTERVAL_SECONDS_OPTION',
  'JSON_OPTION',
  'OPEN_BROWSER_OPTION',
  'SHARDS_OPTION',
  'SSH_USER_OPTION',
  'WAIT_UNTIL_OPTION',
)


def add_verbosity_options():
  from twitter.common import app
  from twitter.common.log.options import LogOptions

  def set_quiet(option, _1, _2, parser):
    setattr(parser.values, option.dest, 'quiet')
    LogOptions.set_stderr_log_level('NONE')

  def set_verbose(option, _1, _2, parser):
    setattr(parser.values, option.dest, 'verbose')
    LogOptions.set_stderr_log_level('DEBUG')

  app.add_option('-v',
                 dest='verbosity',
                 default='normal',
                 action='callback',
                 callback=set_verbose,
                 help='Verbose logging. (default: %default)')

  app.add_option('-q',
                 dest='verbosity',
                 default='normal',
                 action='callback',
                 callback=set_quiet,
                 help='Quiet logging. (default: %default)')



def parse_aurora_job_key_into(option, opt, value, parser):
  try:
    setattr(parser.values, option.dest, AuroraJobKey.from_path(value))
  except AuroraJobKey.Error as e:
    raise optparse.OptionValueError('Failed to parse: %s' % e)


def make_env_option(explanation):
  return optparse.Option(
    '--env',
    dest='env',
    default=None,
    help=explanation)


OPEN_BROWSER_OPTION = optparse.Option(
    '-o',
    '--open_browser',
    dest='open_browser',
    action='store_true',
    default=False,
    help='Open a browser window to the job page after a job mutation.')


SHARDS_OPTION = optparse.Option(
    '--shards',
    type='string',
    dest='shards',
    default=None,
    action='callback',
    callback=parse_shards_into,
    help='A list of shard ids to act on. Can either be a comma-separated list (e.g. 0,1,2) '
    'or a range (e.g. 0-2) or any combination of the two (e.g. 0-2,5,7-9). If not set, '
    'all shards will be acted on.')


FROM_JOBKEY_OPTION = optparse.Option('--from', dest='rename_from', type='string', default=None,
    metavar='CLUSTER/ROLE/ENV/JOB', action='callback', callback=parse_aurora_job_key_into,
    help='Job key to diff against.')


JSON_OPTION = optparse.Option(
    '-j',
    '--json',
    dest='json',
    default=False,
    action='store_true',
    help='If specified, configuration is read in JSON format.')


CLUSTER_CONFIG_OPTION = optparse.Option(
  '--cluster',
  dest='cluster',
  default=None,
  type='string',
  help='Cluster to match when selecting a job from a configuration. Optional if only one job '
       'matching the given job name exists in the config.')


CLUSTER_INVOKE_OPTION = optparse.Option(
  '--cluster',
  dest='cluster',
  default=None,
  type='string',
  help='Cluster to invoke this command against. Deprecated in favor of the CLUSTER/ROLE/ENV/NAME '
       'syntax.')


ENV_CONFIG_OPTION = make_env_option(
  'Environment to match when selecting a job from a configuration.')


# This is for binding arbitrary points in the Thermos namespace to specific strings, e.g.
# if a Thermos configuration has {{jvm.version}}, it can be bound explicitly from the
# command-line with, for example, -E jvm.version=7
ENVIRONMENT_BIND_OPTION = optparse.Option(
    '-E',
    type='string',
    nargs=1,
    action='callback',
    default=[],
    metavar='NAME=VALUE',
    callback=add_binding_to('bindings'),
    dest='bindings',
    help='Bind a thermos mustache variable name to a value. '
         'Multiple flags may be used to specify multiple values.')


EXECUTOR_SANDBOX_OPTION = optparse.Option(
    '-e',
    '--executor_sandbox',
    action='store_true',
    default=False,
    dest='executor_sandbox',
    help='Run the command in the executor sandbox instead of the task sandbox.')


SSH_USER_OPTION = optparse.Option(
    '--user',
    dest='ssh_user',
    default=None,
    help="ssh as this user instead of the role.")


CREATE_STATES = (
  'PENDING',
  'RUNNING',
  'FINISHED'
)


WAIT_UNTIL_OPTION = optparse.Option(
    '--wait_until',
    default='PENDING',
    type='choice',
    choices=('PENDING', 'RUNNING', 'FINISHED'),
    metavar='STATE',
    dest='wait_until',
    help='Block the client until all the tasks have transitioned into the '
         'requested state.  Options: %s.  Default: %%default' % (', '.join(CREATE_STATES)))


HEALTH_CHECK_INTERVAL_SECONDS_OPTION = optparse.Option(
    '--updater_health_check_interval_seconds',
    dest='health_check_interval_seconds',
    type=int,
    default=3,
    help='Time interval between subsequent shard status checks.'
)
