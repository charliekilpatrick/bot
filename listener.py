import json
import os
import operator
import requests
import urllib
import re
import time
import shutil
import datetime
import sys
import pandas
import numpy as np
import healpy as hp
import dustmaps.sfd
from dateutil.parser import parse
from astropy.io import fits,ascii
from astropy.time import Time
from astropy.table import Table
from astropy.coordinates import SkyCoord
from astropy import units as u

import dustmaps.sfd

for pole in ['ngp', 'sgp']:
    datadir=dustmaps.sfd.data_dir()
    file = os.path.join(datadir, 'sfd',
        'SFD_dust_4096_{}.fits'.format(pole))
    if not os.path.exists(file):
        dustmaps.sfd.fetch()
        break

class listener(object):
    def __init__(self):

        self.botname = 'Robotron 2000'
        self.channel = 'grb_alerts'

        self.summary = '*{trig}* ({url})\n'
        self.summary += '*Trigger Time*: {date} {time}\n'
        self.summary += '*BAT Coord*: {ra} {dec}\n'
        self.summary += '*BAT Coord (Galactic l, b)*: {l} {b}\n'
        self.summary += '*XRT Coord*: {ra1} {dec1}\n'
        self.summary += '*MW Extinction (Av)*: {av} mag\n'
        self.summary += '*BAT Light Curve*: {baturl}\n'

        self.url = 'https://gcn.gsfc.nasa.gov/swift_grbs.html'

        self.token_file = '/home/ckilpatrick/scripts/python/grb/slack.token'

        self.events_file = '/home/ckilpatrick/scripts/python/grb/swift_bat.dat'

        self.sfd = dustmaps.sfd.SFDQuery()


    def import_all_events(self):
        r = requests.get(self.url)

        # This is slowest step - maybe a way to speed up?
        df = pandas.read_html(r.content)

        table = None
        header = [key for key in df[0].columns]

        # Sanitize header for astropy
        astrohead = []
        for key in header:
            key = key[1]
            key = key.split('[')[0]
            key = key.replace('90%','')
            key = key.split('(')[0]
            key = key.strip()
            astrohead.append(key)

        for i in np.arange(df[0].shape[0]):
            row = []
            for key in header:
                d = df[0][key].iloc[i]
                # Parse nan values
                if str(d)=='nan': d = ''
                row.append(d)

            # Sanitize
            if row[0]=='Trig': continue
            if not table:
                table = Table([[r] for r in row], names=astrohead)
            else:
                table.add_row(row)

        return(table)

    def parse_event_info(self, table, event):
        # Given a table row, parse out the information for a given event name
        table = table[table['Trig']==event]

        if len(table)!=1:
            return(None)

        row = table[0]
        data = {'trig': event, 'url': self.url,
            'date': row['Date yy/mm/dd'], 'time': row['Time UT'],
            'ra': row['BAT RA'], 'dec': row['BAT Dec'], 'l': '', 'b': '',
            'ra1': row['XRT RA'], 'dec1': row['XRT Dec'], 'av':''}

        if row['BAT RA'] and row['BAT Dec']:
            coord = SkyCoord(row['BAT RA'], row['BAT Dec'],
                unit=(u.deg, u.deg))
            ra, dec = coord.to_string(style='hmsdms', sep=':',
                precision=2).split()
            data['l'] = '%7.4f' % coord.galactic.l.degree
            data['b'] = '%7.4f' % coord.galactic.b.degree
            data['ra'] = ra ; data['dec'] = dec
            data['av'] = '%7.3f'%(self.sfd(coord) * 3.1)

        if row['XRT RA'] and row['XRT Dec']:
            coord = SkyCoord(row['XRT RA'], row['XRT Dec'],
                unit=(u.deg, u.deg))
            ra, dec = coord.to_string(style='hmsdms', sep=':',
                precision=2).split()
            data['ra1'] = ra ; data['dec1'] = dec

        # Parse BAT light curve jpeg url
        baturl='https://gcn.gsfc.nasa.gov/notices_s/sw0{0}000msb.jpeg'
        data['baturl']=baturl.format(str(event))

        return(data)

    def write_out_events(self, table):
        table['Trig'] = table['Trig'].astype(int)
        table.sort('Trig')
        table.write(self.events_file, format='ascii', overwrite=True)

    def read_in_events(self):
        t = Table.read(self.events_file, format='ascii')
        for key in t.keys(): t[key] = t[key].astype(str)
        return(t)

    def slackCommand(self, client, command, **kwargs):
        resp = client.api_call(command, json=kwargs)
        message = '#' * 80 + '\n'
        message += '#' * 80 + '\n'
        message += '################ SLACK COMMAND {cmd} ################\n'
        print(message.format(cmd=command))
        print(resp)
        print('')
        print('')

        return(resp)

    def getSlackToken(self):
        if os.path.exists(self.token_file):
            with open(self.token_file) as f:
                token = f.readline().replace('\n', '')
                os.environ['SLACK_TOKEN'] = token
        else:
            token_file = self.token_file
            print(f'WARNING: token file {token_file} does not exist.')
            print('Create token file or manually set SLACK_TOKEN variable.')

    def setUpSlack(self):
        from slack_sdk import WebClient as slackclient
        self.getSlackToken()
        client = slackclient(os.environ.get('SLACK_TOKEN'))

        kwargs={}

        resp = self.slackCommand(client, 'conversations.list', **kwargs)
        for channel in resp['channels']:
            if self.channel == channel['name'].lower():
                kwargs['channel'] = channel['id']

        return(client, kwargs)

    def postEvent(self, client, kwargs, table, event):

        data = self.parse_event_info(table, event)
        summary = self.summary.format(**data)

        # Post summary data
        kwargs['text'] = summary
        kwargs['channel']=self.channel
        kwargs['username']=self.botname
        kwargs['icon_url']='https://ziggy.ucolick.org/ckilpatrick/images/'+\
            'icon.jpg'
        resp = self.slackCommand(client, 'chat.postMessage', **kwargs)

