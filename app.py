#!/usr/bin/env python3
"""
duffel.py

Flask app for routing to Rail outputs on various hosts. Currently supports only
Amazon Cloud Drive. Requires https://github.com/yadayada/acd_cli is authorized
and set up as owner of shared directory.
"""
from flask import Flask, redirect, render_template, abort, request, Response
from contextlib import closing
import subprocess
import json
import requests
import sys
import gzip
import atexit
import os
import time
import mmh3
import random
app = Flask(__name__)
app.static_folder = 'static'

# Path to log file is hardcoded
# DO NOT TRACK IPs/LOCATIONS; track only times
# Always open new logfile when restarting
_LOGDIR = os.path.expanduser('~/recount3_logs')
os.makedirs(_LOGDIR, exist_ok=True)
filename_numbers = []
for filename_number in [filename.split('.')[1] for filename
    in os.listdir(_LOGDIR) if 'recount3_log' in filename]:
    try:
        filename_numbers.append(int(filename_number))
    except ValueError:
        # Not a recognized log file
        pass
try:
    new_filename_number = max(filename_numbers) + 1
except ValueError:
    # Starting from 0 here
    new_filename_number = 0
_LOGFILE = os.path.join(_LOGDIR,
        'recount3_log.{filename_number}.{rando}.tsv.gz'.format(
        filename_number=new_filename_number,
        rando='{rando}'.format(rando=random.random())[2:]
    ))
_LOGSTREAM = gzip.open(_LOGFILE, 'at')
def close_log():
    """ Closes log stream; for use on script exit.

        No return value.
    """
    _LOGSTREAM.close()
atexit.register(close_log)

@app.route('/')
def recountwebsite():
    return app.send_static_file('index.html')

@app.route('/<resource>/')
@app.route('/<resource>/<path:identifier>')
def forward(resource, identifier=''):
    """ Redirects request for file to direct URL.

        Requires global "paths" dictionary is active. 

        resource: a given resource, like "recount2"
        identifier: relative path to file or directory

        Return value: Flask redirect response object
    """
    # Log all requests, even weird ones
    ip = str(request.headers.get('X-Forwarded-For',
                        request.remote_addr)).split(',')[0].strip()
    print('\t'.join(
        [time.strftime('%A, %b %d, %Y at %I:%M:%S %p %Z'),
             str(mmh3.hash128(ip + 'recountsalt')),
             resource,
             identifier]),
             file=_LOGSTREAM, flush=True)
    print(resource, file=sys.stderr)
    if resource == 'data':
        recdata_url = '/'.join(
                        ['http://methylation.recount.bio',
                          identifier]
                    )
        recdata_response = requests.head(recdata_url)
        if recdata_response.status_code == 200:
            return redirect(recdata_url, code=302)
    abort(404)

if __name__ == '__main__':
    app.run(debug=True)
