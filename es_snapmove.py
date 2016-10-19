#!/usr/bin/python
# Script to move Elasticsearch indexes between clusters using snapshot/restore

import sys
import logging
import argparse
# https://elasticsearch-py.readthedocs.io/en/master/api.html
import elasticsearch
# https://curator.readthedocs.io/en/latest/examples.html
import curator
import json

parser = argparse.ArgumentParser(description='Move Elasticsearch indexes via snapshot/resore')
parser.add_argument('--days-to-keep', type=int, default=3, help='Number of days worth of indices to keep in source cluster (3)')
parser.add_argument('--source-url', default='http://localhost:9200', help='The source Elasticsearch url (http://localhost:9200)')
parser.add_argument('--destination-url', default='http://localhost:9201', help='The destination Elasticsearch url (http://localhost:9201)')
parser.add_argument('--repository', default='es-snapshots', help='The snapshots repository (es-snapshots)')
parser.add_argument('--no-verify-certs', help='Limits the running of the script to the master node only', action="store_true")
parser.add_argument('--dry-run', help='Print actions only', action="store_true")

args = parser.parse_args()

logging.getLogger('curator').addHandler(logging.NullHandler())

def main():

    source_client = elasticsearch.Elasticsearch(args.source_url)
    destination_client = elasticsearch.Elasticsearch(args.destination_url)

    print 'Starting Elasticsearch index move job using:', args.source_url
    latest_snapshot = get_most_recent_snapshot(source_client, args.repository)
    snapshot_indices = get_snapshot_indices(source_client, args.repository, latest_snapshot)
    print 'Running snapshot for:', latest_snapshot
    print '    Containing indices:', snapshot_indices

    working_index = get_next_index_to_move(source_client, args.days_to_keep)

    print 'Next index to move:', working_index
    print 'Doc count in source cluster:', get_index_doc_count(source_client,working_index)
    print 'Doc count in destination cluster:', get_index_doc_count(destination_client,working_index)
    return 0


def get_index_doc_count(client, indexname):
    index_list = curator.IndexList(client)
    return index_list.index_info[indexname]['docs']


def get_indices_to_move(client, days_to_keep):
    index_list = curator.IndexList(client)
    # exclude .kibana, kibana-int, .marvel-kibana, or .marvel-es-data
    index_list.filter_kibana(exclude=True)
    index_list.filter_by_age(
        source='creation_date', direction='older', unit='days', unit_count=days_to_keep
    )
    return index_list


def get_next_index_to_move(client, days_to_keep):
    index_list = get_indices_to_move(client, days_to_keep)
    index_list.filter_by_count(
        count=1, source='creation_date', use_age=True, reverse=False, exclude=False
    )
    return index_list.indices[0]


def get_most_recent_snapshot(client, repository):
    snapshots = curator.snapshotlist.SnapshotList(client, repository)
    return snapshots.most_recent()


def get_snapshot_indices(client, repository, latest_snapshot):
    snap_details = curator.utils.get_snapshot(client, repository, latest_snapshot)
    return snap_details['snapshots'][0]['indices']


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
