#!/usr/bin/env python3

from listener import listener
from optparse import OptionParser
import datetime, os, sys
from astropy.time import Time

def update():
    gdb = listener()
    if os.path.exists(gdb.events_file):
        print('Loading file...')
        events = gdb.read_in_events()
        event_ids = [str(e['Trig']) for e in events]
    else:
        print('Could not find events file:',gdb.events_file)
        return(None)

    do_update = False
    client = None
    kwargs = {}

    newevents = gdb.import_all_events()
    for event in newevents:
        if str(event['Trig']) not in event_ids:
            newrow = [str(event[key]) for key in event.colnames]
            events.add_row(newrow)

            if not client:
                client, kwargs = gdb.setUpSlack()

            gdb.postEvent(client, kwargs, newevents, event['Trig'])
            do_update = True

    if do_update:
        gdb.write_out_events(events)

if __name__ == "__main__":

    update()

