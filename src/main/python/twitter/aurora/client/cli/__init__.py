
from abc import abstractmethod
import argparse
import sys


EXIT_OK = 0
EXIT_INVALID_CONFIGURATION=3
EXIT_COMMAND_FAILURE=4
EXIT_INVALID_COMMAND=5
EXIT_INVALID_PARAMETER=6
EXIT_NETWORK_ERROR=7
EXIT_PERMISSION_VIOLATION=8
EXIT_TIMEOUT=9
EXIT_UNKNOWN_ERROR=20


class Context(object):
  class ArgumentException(Exception): pass

  def set_options(self, options):
    self.options = options

class AuroraCommand(object):
  def setup_options_parser(self, argparser):
    """ Set up command line options parsing for this command.
    This is a thin veneer over the standard python argparse system.
    :param argparser: the argument parser where this command can add its arguments.
    """
    pass

  @property
  def help(self):
    pass

  @property
  def name(self):
    pass

  def __init__(self):
    pass

class CommandLine(object):
  def __init__(self):
    self.nouns = {}
    self.parser = None

  def register_noun(self, noun):
    self.nouns[noun.name] = noun

  def setup_options_parser(self):
    self.parser = argparse.ArgumentParser()
    subparser = self.parser.add_subparsers(dest='noun')
    for (name, noun) in self.nouns.items():
      noun_parser = subparser.add_parser(name, help=noun.help)
      noun.internal_setup_options_parser(noun_parser)

  def register_nouns(self):
    """Override this with an application specific noun registration method."""
    pass

  def execute(self, args):
    self.register_nouns()
    self.setup_options_parser()
    options = self.parser.parse_args(args)
    if options.noun not in self.nouns:
      raise ValueError('Unknown command: %s' % options.noun)
    noun = self.nouns[options.noun]
    context = noun.create_context()
    context.set_options(options)
    noun.execute(context)


class AuroraCommandLine(CommandLine):
  @classmethod
  def get_description(cls):
    return 'Aurora client command line'

  def register_nouns(self):
    from .jobs import Job
    self.register_noun(Job())


class Noun(AuroraCommand):
  class InvalidVerbException(Exception): pass

  def __init__(self):
    super(Noun, self).__init__()
    self.verbs = {}

  def register_verb(self, verb):
    self.foo = 'bar'
    self.verbs[verb.name] = verb
    verb._register(self)

  def internal_setup_options_parser(self, argparser):
    """Internal driver for the options processing framework."""
#    super(self, Noun).internal_setup_options_parser(self, argparser)
    self.setup_options_parser(argparser)
    subparser = argparser.add_subparsers(dest='verb')
    for (name, verb) in self.verbs.items():
      vparser = subparser.add_parser(name, help=verb.help)
      verb.setup_options_parser(vparser)

  @classmethod
  def create_context(self, options):
    pass

  @abstractmethod
  def setup_options_parser(self, argparser):
    pass

  def execute(self, context):
    if context.options.verb not in self.verbs:
      raise self.InvalidVerbException('Noun %s does not have a verb %s' % (self.name, cmd))
    self.verbs[context.options.verb].execute(context)


class Verb(AuroraCommand):
  def __init__(self):
    super(Verb, self).__init__()

  def _register(self, noun):
    self.noun = noun

  @abstractmethod
  def setup_options_parser(self, argparser):
   pass

  def execute(self, context):
    pass


def main():
  cmd = AuroraCommandLine()
  return cmd.execute(sys.argv[1:])

if __name__ == '__main__':
  main(sys.argv)

