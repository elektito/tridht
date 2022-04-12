"""Add a announces table

Revision ID: 63b0f1ddc16a
Revises: 021a37eb7e72
Create Date: 2022-04-11 08:04:10.144975

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '63b0f1ddc16a'
down_revision = '021a37eb7e72'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'announces',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('ih', sa.LargeBinary(), nullable=True),
        sa.Column('node_id', sa.LargeBinary(), nullable=True),
        sa.Column('time', sa.DateTime(), nullable=True),
        sa.Column('peer_ip', postgresql.INET(), nullable=True),
        sa.Column('peer_port', sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade():
    op.drop_table('announces')
