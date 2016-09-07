#!/usr/bin/env python

import os
import logging
import gzip
import pickle
from collections import namedtuple
from debian import deb822, debfile
import requests
from tempfile import mkstemp
from queue import Queue
from threading import Thread
import sys
import shutil
from urllib.request import urlopen
from collections import defaultdict

# New
import MySQLdb


package_dir = os.path.dirname(os.path.abspath(__file__))
pjoin = os.path.join
bname = os.path.basename

normalize_member = debfile.DebPart._DebPart__normalize_member

PackageName = namedtuple("PackageName", ['section', 'package'])
Package = namedtuple("Package", ['url', 'desc'])

class Parser(object):
    db_user = "root"
    db_host = "localhost"
    db_database = "cartatech"

    def __init__(self):
        self.conn = MySQLdb.connect(host = self.db_host, db = self.db_database, user = self.db_user)

    def loaddb(self):
        logging.info("Rebuilding cache")
        DebianRepo.ensure_packages_contents_exist()

        cursor = self.conn.cursor()
        packages_with_pages = set()
        query = "INSERT INTO manpages (path, name, section, package) VALUES (%s, %s, %s, %s)"
        with gzip.open(DebianRepo.contents_file, mode='rt') as fp:
            in_header = True
            for line in fp:
                if in_header:
                    if line.startswith('FILE'):
                        in_header = False
                    continue

                path, location = line.rsplit(None, 1)

                if not is_manpage(path):
                    continue

                name, section = manpage_name(path).rsplit('.', 1)

                for p in location.split(','):
                    _, package = p.split('/')

                    cursor.execute(query, (path, name, section, package, ))
                    packages_with_pages.add(package)

        self.conn.commit()
        cursor.close()

        cursor = self.conn.cursor()
        query = "INSERT INTO packages (package, filename, version, description) VALUES (%s, %s, %s, %s)"
        with gzip.open(DebianRepo.packages_file, mode='rt') as fp:
            for b in deb822.Packages.iter_paragraphs(sequence=fp):
                if b['Package'] not in packages_with_pages:
                    continue

                cursor.execute(query, (b['Package'], b['Filename'], b['Version'], b['Description'], ))

        self.conn.commit()
        cursor.close()


class DebianRepo(object):
    """docstring for DebianRepo"""
    cache_dir = pjoin(package_dir, "cache")
    cache_file = pjoin(cache_dir, "package-cache")

    contents_file = pjoin(cache_dir, "Contents")
    packages_file = pjoin(cache_dir, "Packages")

    base_url = "http://httpredir.debian.org/debian/"
    contents_url = base_url + "dists/jessie/main/Contents-i386.gz"
    packages_url = base_url + "dists/jessie/main/binary-i386/Packages.gz"

    @staticmethod
    def create_cache_dir():
        if not os.path.isdir(DebianRepo.cache_dir):
            logging.debug("Creating non existing cache directory")
            os.makedirs(DebianRepo.cache_dir)

    @staticmethod
    def update():
        logging.info("Fetching updated Packages and Contents files")
        DebianRepo.create_cache_dir()

        logging.info("Fetching contents file")
        if os.path.isfile(DebianRepo.contents_file):
            logging.debug("Removing old copy of contents file")
            os.unlink(DebianRepo.contents_file)

        req = urlopen(DebianRepo.contents_url)
        with open(DebianRepo.contents_file, 'wb') as fp:
            shutil.copyfileobj(req, fp)

        logging.info("Fetching packages file")
        if os.path.isfile(DebianRepo.packages_file):
            logging.debug("Removing old copy of packages file")
            os.unlink(DebianRepo.packages_file)

        req = urlopen(DebianRepo.packages_url)
        with open(DebianRepo.packages_file, 'wb') as fp:
            shutil.copyfileobj(req, fp)

    @staticmethod
    def rebuild_cache():
        logging.info("Rebuilding cache")
        DebianRepo.ensure_packages_contents_exist()
        DebianRepo.create_cache_dir()

        if os.path.isfile(DebianRepo.cache_file):
            logging.debug("Removing old cache")
            os.unlink(DebianRepo.cache_file)

        with gzip.open(DebianRepo.contents_file, mode='rt') as fp:
            in_header = True
            packages = defaultdict(DebianPackage)
            for line in fp:
                if in_header:
                    if line.startswith('FILE'):
                        in_header = False
                    continue

                file, location = line.rsplit(None, 1)

                if not is_manpage(file):
                    continue

                for p in location.split(','):
                    section, package = p.split('/')
                    current_package = PackageName(section, package)
                    packages[current_package].add_member(file)

        with gzip.open(DebianRepo.packages_file, mode='rt') as fp:
            for b in deb822.Packages.iter_paragraphs(sequence=fp):
                p = PackageName(b['Section'], b['Package'])

                if p not in packages:
                    continue

                packages[p]['ver'] = b['Version']
                packages[p]['url'] = b['Filename']
                packages[p]['desc'] = b['Description']

        with open(DebianRepo.cache_file, 'wb') as f:
            pickle.dump(packages, f)

    @staticmethod
    def ensure_packages_contents_exist():
        if not os.path.isfile(DebianRepo.contents_file) or not os.path.isfile(
                DebianRepo.packages_file):
            logging.error("Missing contents or packages files. Please update")
            raise IOError

    @staticmethod
    def update_cache():
        logging.info("Updating cache")

        if not os.path.isfile(DebianRepo.cache_file):
            logging.warning("Cache does not exist, rebuiliding it...")
            DebianRepo.rebuild_cache()
            return

        DebianRepo.ensure_packages_contents_exist()

        with open(DebianRepo.cache_file, 'rb') as f:
            packages = pickle.load(f)

        with gzip.open(DebianRepo.contents_file, mode='rt') as fp:
            in_header = True

            packages_with_pages = set()
            for line in fp:
                if in_header:
                    if line.startswith('FILE'):
                        in_header = False
                    continue

                file, location = line.rsplit(None, 1)

                if not is_manpage(file):
                    continue

                for p in location.split(','):
                    section, package = p.split('/')
                    current_package = PackageName(section, package)
                    packages_with_pages.add(current_package)
                    packages[current_package].add_member(file)

        for k in set(packages) - packages_with_pages:
            packages[k]['todelete'] = True

        with gzip.open(DebianRepo.packages_file, mode='rt') as fp:
            for b in deb822.Packages.iter_paragraphs(sequence=fp):
                p = PackageName(b['Section'], b['Package'])

                if p not in packages:
                    continue

                if packages[p]['ver'] != b['Version']:
                    packages[p]['ver'] = b['Version']
                    packages[p]['url'] = b['Filename']
                    packages[p]['desc'] = b['Description']
                    packages[p]['flushed'] = False

        with open(DebianRepo.cache_file, 'wb') as f:
            pickle.dump(packages, f)


class DebianPackage(dict):
    def __init__(self, *args):
        dict.__init__(self, args)
        self['flushed'] = False

    def add_member(self, member):
        if 'members' not in self:
            self['members'] = defaultdict(dict)

        member = normalize_member(member)

        if member not in self['members']:
            self['flushed'] = False
            self['members'][member]['name'] = manpage_name(member)
            self['members'][member]['state'] = 0


class DebianManpageFetcher(object):
    output_dir = pjoin(package_dir, "output")
    base_url = "http://httpredir.debian.org/debian/"

    packages = None

    @staticmethod
    def load_packages():
        if DebianManpageFetcher.packages is None:
            with open(DebianRepo.cache_file, 'rb') as f:
                DebianManpageFetcher.packages = pickle.load(f)

    @staticmethod
    def save_packages():
        if DebianManpageFetcher.packages is not None:
            with open(DebianRepo.cache_file, 'wb') as f:
                pickle.dump(DebianManpageFetcher.packages, f)

    @staticmethod
    def process_package(q):
        while True:
            DebianManpageFetcher.get_manpages_from_package(q.get())
            q.task_done()

    @staticmethod
    def fetchall():
        logging.debug("Fetching all manpages")
        DebianManpageFetcher.load_packages()

        if not os.path.isdir(DebianManpageFetcher.output_dir):
            os.makedirs(DebianManpageFetcher.output_dir)

        q = Queue(maxsize=0)
        num_threads = 50

        for i in range(num_threads):
            worker = Thread(target=DebianManpageFetcher.process_package, args=(q, ))
            worker.setDaemon(True)
            worker.start()

        for (section, package), v in DebianManpageFetcher.packages.items():
            if v['flushed']:
                continue

            q.put(package)

        q.join()

        DebianManpageFetcher.save_packages()

    @staticmethod
    def fetchone(package):
        logging.debug("Fetching manpages in %s", package)
        DebianManpageFetcher.load_packages()
        DebianManpageFetcher.get_manpages_from_package(package)
        DebianManpageFetcher.save_packages()

    @staticmethod
    def get_manpages_from_package(package):
        logging.debug("Fetching manpages in %s", package)

        container = pjoin(DebianManpageFetcher.output_dir, package)
        if not os.path.isdir(container):
            os.makedirs(container)

        # TODO: We will take only the first one
        # (edge case with duplicated names in sectons)
        p = [sp for sp in DebianManpageFetcher.packages if sp.package == package][0]
        cp = DebianManpageFetcher.packages[p]

        if cp['flushed']:
            return True

        url = DebianManpageFetcher.base_url + cp['url']
        response = requests.get(url, stream = True)

        _, tmpfile = mkstemp()

        fp = open(tmpfile, 'wb')
        fp.write(response.content)
        fp.close()

        try:
            data_file = debfile.DebFile(tmpfile).data
        except:
            print("Error processing (A) {}".format(package))
            os.unlink(tmpfile)
            return

        for member, v in cp['members'].items():
            if v['state']:
                continue

            item = data_file.tgz().getmember('./' + member)

            try:
                file_contents = data_file.get_file(member)
            except KeyError:
                if item.issym():
                    # TODO: Do something with missing links
                    print("Saved {}".format(package))
                    v['state'] = 2
                    continue
                else:
                    return True

            compressed = member.endswith(".gz")
            if compressed:
                file_contents = gzip.GzipFile(fileobj=file_contents)

            if item.issym():
                cp['members'][member]['link'] = manpage_name(
                    item.linkname)

            final_path = pjoin(container, v['name'])
            fp = open(final_path, "wb")
            fp.write(file_contents.read())
            fp.close()

            v['state'] = 1
        else:
            cp['flushed'] = True

        os.unlink(tmpfile)

# Helper functions
def manpage_name(file):
    if '/' in file:
        file = bname(file)

    if file.endswith('.gz'):
        file = file[:-3]

    return file


def is_manpage(file):
    man_occ = file.count('/man')

    if not man_occ:
        return False

    name = bname(file)

    if '.' not in name:
        return False

    name = manpage_name(name)

    if '.' not in name:
        return False

    if man_occ > 1:
        if '/man/man' not in file:
            # Localized man page
            return False

    _, ext = os.path.splitext(name)

    if not ext[1:2].isdigit():
        return False

    return True


## Argument parsers
def fetchall(args):
    DebianManpageFetcher.fetchall()


def fetchone(args):
    DebianManpageFetcher.fetchone(args.package)


def update(args):
    DebianRepo.update()


def rebuild_cache(args):
    DebianRepo.rebuild_cache()


def update_cache(args):
    DebianRepo.update_cache()

# New
def loaddb(args):
    p = Parser()
    p.loaddb()


if __name__ == '__main__':
    import time
    import argparse
    start_time = time.time()

    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", help="choose log level")

    subparsers = parser.add_subparsers()

    # update option
    parser_update = subparsers.add_parser(
        'update',
        help='Updates Packages and Contents',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser_update.set_defaults(func=update)

    # loaddb option
    parser_loaddb = subparsers.add_parser(
        'loaddb',
        help='Loads Information in the Database',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser_loaddb.set_defaults(func=loaddb)

    # rebuild_cache option
    parser_rebuild_cache = subparsers.add_parser(
        'rebuild-cache',
        help='Rebuilds package and content cache from scratch',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser_rebuild_cache.set_defaults(func=rebuild_cache)

    # update_cache option
    parser_update_cache = subparsers.add_parser(
        'update-cache',
        help='Updates package and content cache',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser_update_cache.set_defaults(func=update_cache)

    # fetchall option
    parser_fetchall = subparsers.add_parser(
        'fetchall',
        help='Fetch the entire mirror',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser_fetchall.set_defaults(func=fetchall)

    # fetchone option
    parser_fetchone = subparsers.add_parser(
        'fetchone',
        help='Fetch one package',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser_fetchone.add_argument("package", help="the package to fetch")
    parser_fetchone.set_defaults(func=fetchone)

    args = parser.parse_args()

    try:
        a = getattr(args, "func")
    except AttributeError:
        parser.print_help()
        sys.exit(0)

    if args.log_level:
        log_level = getattr(logging, args.log_level.upper())
        logging.basicConfig(level=log_level)

    args.func(args)

    elapsed = time.time() - start_time
    logging.info("--- Total time: %s seconds ---" % (elapsed, ))
