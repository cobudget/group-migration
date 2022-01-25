from pprint import pprint
import psycopg2
import logging
import json
import os

mylogs = logging.getLogger(__name__)
mylogs.setLevel(logging.DEBUG)

file = logging.FileHandler("cobudget-group-export.log")
file.setLevel(logging.INFO)
fileformat = logging.Formatter("%(asctime)s:%(levelname)s: %(message)s",datefmt="%H:%M:%S")
file.setFormatter(fileformat)

stream = logging.StreamHandler()
stream.setLevel(logging.DEBUG)
streamformat = logging.Formatter("%(asctime)s: %(message)s")
stream.setFormatter(streamformat)

mylogs.addHandler(file)
mylogs.addHandler(stream)

def get_group(db_cursor, group_id):

    mylogs.info(f'Loading data for group {group_id}')

    group_query = f'''
    SELECT
    name, created_at, updated_at, currency_code, description, status_account_id
    FROM groups
    WHERE id = {group_id}
    '''

    db_cursor.execute(group_query)
    group_data = db_cursor.fetchall()
    g = group_data[0]

    group = {
        'id': group_id,
        'name': g[0],
        'created_at': g[1],
        'updated_at': g[2],
        'currency_code': g[3],
        'description': g[4],
        'status_account_id': g[5],
    }

    return group

def get_buckets(db_cursor, group_id):

    mylogs.info(f'Loading buckets for group {group_id}')

    buckets_query = f'''
    SELECT
    id, created_at, updated_at, name, description, user_id, target, status, funding_closes_at, funded_at, live_at, archived_at, paid_at, account_id
    FROM buckets
    WHERE group_id = {group_id}
    '''

    db_cursor.execute(buckets_query)
    buckets_data = db_cursor.fetchall()
    buckets = {}

    for b in buckets_data:
        bucket = {
            'id': b[0],
            'created_at': b[1],
            'updated_at': b[2],
            'name': b[3],
            'description': b[4],
            'user_id': b[5],
            'target:': b[6],
            'status:': b[7],
            'funding_closes_at:': b[8],
            'funded_at:': b[9],
            'live_at:': b[10],
            'archived_at:': b[11],
            'paid_at:': b[12],
            'account_id:': b[13],
        }
        buckets[b[0]] = bucket

    return buckets

def get_members(db_cursor, group_id):

    mylogs.info(f'Loading members and users for group {group_id}')

    members_query = f'''
    SELECT
    memberships.id, group_id, member_id, is_admin, memberships.created_at, memberships.updated_at, status_account_id, incoming_account_id, outgoing_account_id, users.id, users.email, users.name, users.uid 
    FROM memberships 
    INNER JOIN users
    ON memberships.member_id = users.id
    WHERE group_id = {group_id}
    '''

    db_cursor.execute(members_query)
    members_data = db_cursor.fetchall()
    members = {}

    for m in members_data:
        member = {
            'id': m[0], 
            'group_id': m[1], 
            'member_id': m[2], 
            'is_admin': m[3], 
            'created_at': m[4], 
            'updated_at': m[5], 
            'status_account_id': m[6], 
            'incoming_account_id': m[7], 
            'outgoing_account_id': m[8], 
            'user_id': m[9], 
            'user_email': m[10], 
            'user_name': m[11], 
            'user_uid': m[12]
        }
        members[m[0]] = member
    
    return members

def get_allocations(db_cursor, group_id):

    mylogs.info(f'Loading allocations for group {group_id}')

    allocation_query = f'''
    SELECT
    id, created_at, updated_at, user_id, amount, group_id
    FROM allocations
    WHERE group_id =  {group_id}
    '''

    db_cursor.execute(allocation_query)
    allocation_data = db_cursor.fetchall()
    allocations = {}

    for a in allocation_data:
        allocation = {
            'id': a[0], 
            'created_at': a[1], 
            'updated_at': a[2], 
            'user_id': a[3], 
            'amount': a[4], 
            'group_id': a[5]
        }
        allocations[a[0]] = allocation

    return allocations

def get_contributions(db_cursor, group_id):

    mylogs.info(f'Loading contributions for group {group_id}')

    contribution_query = f'''
    SELECT
    contributions.id, contributions.created_at, contributions.updated_at, contributions.user_id, amount, bucket_id, buckets.group_id
    FROM contributions
    INNER JOIN buckets 
    ON contributions.bucket_id = buckets.id 
    WHERE buckets.group_id =  {group_id}
    '''

    db_cursor.execute(contribution_query)
    contribution_data = db_cursor.fetchall()
    contributions = {}

    for a in contribution_data:
        contribution = {
            'id': a[0], 
            'created_at': a[1], 
            'updated_at': a[2], 
            'user_id': a[3], 
            'amount': a[4], 
            'bucket_id': a[5]
        }
        contributions[a[0]] = contribution

    return contributions

def get_accounts(db_cursor, group_id):

    mylogs.info(f'Loading accounts for group {group_id}')

    account_query = f'''
    SELECT
    id, group_id, created_at, updated_at
    FROM accounts
    WHERE group_id = {group_id}
    '''

    db_cursor.execute(account_query)
    account_data = db_cursor.fetchall()
    accounts = {}

    for a in account_data:
        account = {
            'id': a[0], 
            'group_id': a[1], 
            'created_at': a[2], 
            'updated_at': a[3]
        }
        accounts[a[0]] = account
    
    return accounts


def get_transactions(db_cursor, group_id):

    mylogs.info(f'Loading transactions for group {group_id}')

    transaction_query = f'''
    SELECT
    transactions.id, transactions.created_at, transactions.updated_at, user_id, amount, from_account_id, to_account_id
    FROM transactions
    INNER JOIN accounts 
    ON transactions.from_account_id = accounts.id
    WHERE accounts.group_id =  {group_id}
    '''

    db_cursor.execute(transaction_query)
    transaction_data = db_cursor.fetchall()
    transactions = {}

    for a in transaction_data:
        transaction = {
            'id': a[0], 
            'created_at': a[1], 
            'updated_at': a[2], 
            'user_id': a[3], 
            'amount': a[4], 
            'from_account_id': a[5],
            'to_account_id': a[6]
        }
        transactions[a[0]] = transaction

    return transactions

def get_comments(db_cursor, group_id):

    mylogs.info(f'Loading comments for group {group_id}')

    comment_query = f'''
    SELECT
    comments.id, body, comments.user_id, bucket_id, comments.created_at, comments.updated_at
    FROM comments
    INNER JOIN buckets
    ON buckets.id = comments.bucket_id
    WHERE buckets.group_id = {group_id}
    '''

    db_cursor.execute(comment_query)
    comment_data = db_cursor.fetchall()
    comments = {}

    for c in comment_data:
        comment = {
            'id': c[0], 
            'body': c[1], 
            'user_id': c[2], 
            'bucket_id': c[3], 
            'created_at': c[4], 
            'updated_at': c[5]
        }
        comments[c[0]] = comment
    
    return comments


# Get group data

with open("./config.json") as json_config:
    config = json.load(json_config)

group_ids = config['groups']

db_conn = psycopg2.connect(
    host=config['host'], 
    port=config['port'], 
    dbname=config['dbname'], 
    user=config['user'], 
    password=config['password'],
)

db_cursor = db_conn.cursor()

groups = {}
for gid in group_ids:
    groups[gid] = {
        'group_data': get_group(db_cursor, gid),
        'buckets': get_buckets(db_cursor, gid),
        'members': get_members(db_cursor, gid),
        'accounts': get_accounts(db_cursor, gid),
        'allocations': get_allocations(db_cursor, gid),
        'contributions': get_contributions(db_cursor, gid),
        'transactions': get_transactions(db_cursor, gid),
        'comments': get_comments(db_cursor, gid)
    }

with open(f'./group_exports.json', 'w') as file:
    json.dump(groups, file, default=str)