"""Add parsed metadata

Revision ID: 2701e94edda2
Revises: ed03f77d26b5
Create Date: 2022-04-10 04:32:18.007237

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

import time
import json
from tridht.bencode import bdecode
from tridht.utils import metadata_to_json

# revision identifiers, used by Alembic.
revision = '2701e94edda2'
down_revision = 'ed03f77d26b5'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('infohashes',
                  sa.Column('parsed_metadata',
                            postgresql.JSONB(astext_type=sa.Text()),
                            nullable=True))

    print('Migrating data...')

    print('Fetching data from database...')
    bind = op.get_bind()
    session = sa.orm.Session(bind=bind)
    results = session.execute(
        'select ih, metadata from infohashes where metadata is not null')

    print(f'Parsing metadata in {results.rowcount} rows...')
    last_report = time.monotonic()
    i = 0
    for ih, metadata in results:
        metadata, _ = bdecode(bytes(metadata))
        md_json = metadata_to_json(metadata)
        i += 1
        if time.monotonic() - last_report >= 2:
            print(f'Parsed {i} out {results.rowcount} items so far.')
            last_report = time.monotonic()

        session.execute(
            'update infohashes set parsed_metadata = :md where ih = :ih',
            {'md': json.dumps(md_json), 'ih': ih})

    print('Committing...')
    session.commit()

    print('Data migration finished.')


def downgrade():
    op.drop_column('infohashes', 'parsed_metadata')
