import sys
__all__ = ['archiver', 'crawler']

def report_progress(message):
    """ Print message to stout """
    sys.stdout.write('\r{}'.format(message))
    sys.stdout.flush()