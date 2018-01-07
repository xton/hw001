#!/usr/bin/env python

import sys
import email.parser
from dateutil.parser import parse as dateparse
from dateutil.tz import tzlocal
from datetime import datetime, timedelta
import operator
from collections import defaultdict, namedtuple

EmailRecord = namedtuple('EmailRecord', ['sender', 'recipients', 'msgdate',
                                         'day', 'subject', 'words', 'defects',
                                         'filename'])

# choosing "right now" as default, obviously-wrong time for expediency
a_bad_time = str(datetime.now(tzlocal()))

# don't bother looking for a response after this amount of time,
# it most likely won't break the top 5
max_response_lag = timedelta(minutes=30)


def read_mail_file(filename):
    """parse and extract features from the given email file"""

    headers = email.parser.Parser().parse(open(filename, "r"), headersonly=True)
    subject = headers['subject']
    words = set(subject.split())

    sender = headers['from']

    # NB: split-n-strip is not the most robust way to parse emails out of a list. We're
    #     hoping that the email-normalization that was done to this dataset makes up for that.
    raw_recipients = headers.get_all('to', []) + \
                     headers.get_all('cc', []) + \
                     headers.get_all('bcc', [])
    recipients = set(address.strip()
                     for field in raw_recipients
                     for address in field.split(","))

    msgdate = dateparse(headers.get('date', a_bad_time))
    day = msgdate.strftime("%Y-%m-%d")

    return EmailRecord(sender, recipients, msgdate, day,
                       subject, words, headers.defects, filename)


def main(args):
    data = map(read_mail_file,args)

    emails_per_person_day = defaultdict(lambda: 0)
    directly_received = defaultdict(lambda: 0)
    broadcasted = defaultdict(lambda: 0)

    for record in data:
        for p in record.recipients:
            emails_per_person_day[(p, record.day)] += 1

        if len(record.recipients) > 1:
            broadcasted[record.sender] += 1
        elif len(record.recipients) == 1:
            directly_received[list(record.recipients)[0]] += 1

    print
    print "Daily Counts:"
    for (p, d), n in sorted(emails_per_person_day.iteritems(), key=operator.itemgetter(0)):
        print "% 8d : %s - %s" % (n, d, p)

    name, broadcasted = max(broadcasted.iteritems(), key=operator.itemgetter(1))
    print
    print "Most Broadcasts Sent: %s (%d)" % (name, broadcasted)

    name, directed = max(directly_received.iteritems(), key=operator.itemgetter(1))
    print
    print "Most Directs Received: %s (%d)" % (name, directed)

    print
    print "Fastest Replies:"
    sorted_data = sorted(data, key=lambda x: x.msgdate)

    fast_replies = []
    for idx, record in enumerate(sorted_data):
        cursor = idx - 1
        while cursor > 0:
            first_candidate = sorted_data[cursor]
            response_lag = record.msgdate - first_candidate.msgdate
            if response_lag > max_response_lag:
                # this condition keeps this search fairly constant per message, assuming
                # there are no hotspots in the time distribution and we don't have an
                # unbounded number of messages assigned to the default time.
                break

            if response_lag > timedelta(seconds=0) and \
                    len(first_candidate.subject.strip()) > 0 and \
                    record.sender in first_candidate.recipients and \
                    first_candidate.words <= record.words:

                fast_replies.append((record, response_lag, first_candidate))
                # just one original to each reply
                break

            cursor -= 1

    fastest_replies = sorted(fast_replies, key=operator.itemgetter(1))[:5]
    for idx, (record, response_lag, first_candidate) in enumerate(fastest_replies):
        print "%d: %s [%s] - %s [%s] <= %s [%s]" % \
              ( idx + 1, record.msgdate, response_lag, record.subject, record.filename,
                first_candidate.subject, first_candidate.filename )

    print
    print "DEFECTS:"
    for record in data:
        if record.defects:
            print "%s: %s" % (record.filename, record.defects)


if __name__ == '__main__':
    main(sys.argv[1:])
