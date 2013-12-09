import argparse
import sys

from twitter.aurora.client.cli import Context, EXIT_NETWORK_ERROR
from twitter.aurora.common.aurora_job_key import AuroraJobKey
from twitter.aurora.client.config import get_config
from twitter.aurora.client.base import (
  check_and_log_response,
  synthesize_url)
from twitter.aurora.client.factory import make_client
from twitter.common import log

from gen.twitter.aurora.ttypes import ResponseCode

class AuroraCommandContext(Context):
  def get_packer(self, cluster):
    pass

  def check_and_log_response(self, resp):
    log.info('Response from scheduler: %s (message: %s)'
        % (ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
    if resp.responseCode != ResponseCode.OK:
      sys.exit(EXIT_NETWORK_ERROR)

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
    sys.exit(code)
