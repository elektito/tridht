"""Add a nodes table

Revision ID: 021a37eb7e72
Revises: 2701e94edda2
Create Date: 2022-04-10 21:39:18.174415

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '021a37eb7e72'
down_revision = '2701e94edda2'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'nodes',
        sa.Column('id', sa.LargeBinary(), nullable=False),
        sa.Column('ip', postgresql.INET(), nullable=False),
        sa.Column('port', sa.Integer(), nullable=False),
        sa.Column('last_response_time', sa.DateTime(), nullable=True),
        sa.Column('last_query_time', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade():
    op.drop_table('nodes')
