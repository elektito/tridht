"""Initial migration

Revision ID: ed03f77d26b5
Revises:
Create Date: 2022-04-06 12:05:30.167393

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'ed03f77d26b5'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        '''
        create or replace function ih_score (get_peers int, announces int, samples int, fetch_failures int)
               returns int
               immutable
               language plpgsql
               as $func$
        declare score int;
        begin
            score = 0;

            if get_peers > 5 then
               score = score + 2 * get_peers;
            else
               score = score + get_peers;
            end if;

            score = score + 10 * announces + 2 * samples - fetch_failures;
            return score;
        end
        $func$;
        '''
    )
    op.create_table(
        'infohashes',
        sa.Column('ih',
                  postgresql.BYTEA(),
                  autoincrement=False,
                  nullable=False),
        sa.Column('get_peers',
                  sa.INTEGER(),
                  server_default=sa.text('0'),
                  autoincrement=False,
                  nullable=False),
        sa.Column('announces',
                  sa.INTEGER(),
                  server_default=sa.text('0'),
                  autoincrement=False,
                  nullable=False),
        sa.Column('samples',
                  sa.INTEGER(),
                  server_default=sa.text('0'),
                  autoincrement=False,
                  nullable=False),
        sa.Column('last_announce_time',
                  postgresql.TIMESTAMP(),
                  autoincrement=False,
                  nullable=True),
        sa.Column('last_announce_ip',
                  postgresql.INET(),
                  autoincrement=False,
                  nullable=True),
        sa.Column('last_announce_port',
                  sa.INTEGER(),
                  autoincrement=False,
                  nullable=True),
        sa.Column('metadata',
                  postgresql.BYTEA(),
                  autoincrement=False,
                  nullable=True),
        sa.Column('fetch_due_time',
                  postgresql.TIMESTAMP(),
                  server_default=sa.text('now()'),
                  autoincrement=False, nullable=False),
        sa.Column('fetch_failures',
                  sa.INTEGER(),
                  server_default=sa.text('0'),
                  autoincrement=False, nullable=False),
        sa.Column('score',
                  sa.INTEGER(),
                  sa.Computed(
                      'ih_score(get_peers, announces, samples, fetch_failures)',
                      persisted=True),
                  autoincrement=False,
                  nullable=True),
        sa.PrimaryKeyConstraint('ih', name='infohashes_pkey')
    )


def downgrade():
    op.drop_table('infohashes')
    op.execute('drop function ih_score')
