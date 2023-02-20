#!/usr/bin/env python

# Requires https://github.com/bitly/bitly-api-python

import csv
import getopt
import requests
import sys
import urllib.parse
import datetime

def main(argv=None):
    """
    This script exports all Bit.ly urls for an account.
    For each url it extracts the Bit.ly 'link' which uses the hash Bit.ly generates (e.g. http://bit.ly/1dfPmzu), 'title' and
    'created_at'

    Required options:
        -l, --login=: Bit.ly login
        -p, --password=: Bit.ly Password (used to generate OAuth token)

    Optional parameters:
        -v: Verbose mode
        -h: Display this help and exit
        -u, --user: Export data for a user other than the login user
    """
    # Load options using getopt
    if argv is None:
        argv = sys.argv

    try:
        opts, args = getopt.getopt(
            argv[1:],
            "vhl:p:u:o:",
            ["help", "login=", "password=", "user=", "output="]
        )
    except getopt.error as err:
        print("Option parsing error: %s" % str(err))
        return 2

    # Setup defaults
    verbose = False
    login = None
    password = None
    user = None
    output_path = 'links.csv'

    try:
        for option, value in opts:
            if option == "-v":
                verbose = True
            elif option in ("-h", "--help"):
                print(main.__doc__)
                return 0
            elif option in ("-l", "--login"):
                login = value
            elif option in ("-p", "--password"):
                password = value
            elif option in ("-u", "--user"):
                user = value
            elif option in ("-o", "--output"):
                output_path = value
            else:
                raise Exception('unknown option')
    except Exception as e:
        print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(e)
        print >> sys.stderr, "\t for help use --help"
        return 2

    if login is None:
        raise Exception('Login parameter must be present.')

    if password is None:
        raise Exception('Password parameter must be present.')

    bitly = Bitly(login, password, verbose)

    size = 100
    page = 1
    result_count = 0
    nb_found = 0

    csv_writer = csv.writer(open(output_path, 'w'), quoting=csv.QUOTE_ALL)
    csv_writer.writerow(('Link', 'Title', 'Long url', 'tags', 'created'))

    while nb_found <= result_count:
        data = bitly.user_link_history(size=size, page=page, user=user)

        result_count = data['pagination']['total']

        for link in data['links']:
            link_datetime = datetime.datetime.strptime(link['created_at'], '%Y-%m-%dT%H:%M:%S%z')
            # link_datetime = datetime.datetime.fromisoformat(link['created_at'])

            csv_writer.writerow(
                (
                    link['link'],
                    link.get('title', ''),
                    link['long_url'],
                    ', '.join(link['tags'] or []),
                    link_datetime
                )
            )

            nb_found += 1

        page += 1

        if verbose:
            progress = float(page * size) / result_count

            sys.stdout.write("\r(%2d%%) Loaded %5d/%5d links..." % (
                round(progress * 100), nb_found, result_count
            ))
            sys.stdout.flush()
        if nb_found >= result_count:
            break

    if verbose:
        print("")
        print("Done! Found %d links, expected %d." % (nb_found, result_count))

class Bitly(object):
    def __init__(self, login, password, verbose=False):
        super(Bitly, self).__init__()
        self.login = login
        self.verbose = verbose

        self.access_token = requests.post(
            'https://api-ssl.bitly.com/oauth/access_token',
            auth=(login, password),
            timeout=10
        ).text

        if verbose:
            print("Access token retrieved: %s" % self.access_token)

    def user_link_history(self, size=100, page=1, user=None):
        params = {
            'size': size,
            'page': page,
            'archived': 'both',
            'created_before': 1617261372
        }

        if user is not None:
            params['user'] = user

        return self._call('v4/groups/Bf********s/bitlinks', params)

    def _call(self, method, params):
        """
        A good chunk of the following method has been extracted from
        https://github.com/bitly/bitly-api-python
        https://web.archive.org/web/20221003041941/https://www.jasongaylord.com/blog/2020/10/08/export-links-from-bitly
        https://www.timestamp-converter.com/
        """
        # default to json
        params['format'] = params.get('format','json')

        params['access_token'] = self.access_token

        # force to utf8 to fix ascii codec errors
        encoded_params = []
        for k,v in params.items():
            if type(v) is tuple or type(v) is list:
                v = [e.encode('UTF8') for e in v]
            else:
                v = str(v).encode('UTF8')
            encoded_params.append((k,v))
        params = dict(encoded_params)

        request = "https://api-ssl.bitly.com/%(method)s?%(params)s" % {
            'method': method,
            'params': urllib.parse.urlencode(params, doseq=True)
        }

        http_response = requests.get(
            request,
            headers={"Authorization": "Bearer %s" % self.access_token},
            timeout=10
        )

        if http_response.status_code != 200:
            raise Exception('HTTP %d Error: %s' % (http_response.status_code, http_response.text))

        data = http_response.json()

        if data is None or http_response.status_code != 200:
            raise Exception('Bitly returned error code %d: %s' % (http_response.status_code, str(data)))

        return data

if __name__ == '__main__':
    sys.exit(main())
