"""
Initial schema.
"""

from yoyo import step

__depends__ = {}

steps = [
    step('''
    CREATE FUNCTION ih_score(get_peers integer, announces integer, samples integer, fetch_failures integer) RETURNS integer
    LANGUAGE plpgsql IMMUTABLE
    AS $$
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
        $$;
    '''),
    step('''
    CREATE TABLE infohashes (
        ih bytea NOT NULL PRIMARY KEY,
        get_peers integer DEFAULT 0 NOT NULL,
        announces integer DEFAULT 0 NOT NULL,
        samples integer DEFAULT 0 NOT NULL,
        last_announce_time timestamp without time zone,
        last_announce_ip inet,
        last_announce_port integer,
        metadata bytea,
        fetch_due_time timestamp without time zone DEFAULT now() NOT NULL,
        fetch_failures integer DEFAULT 0 NOT NULL,
        score integer GENERATED ALWAYS AS (ih_score(get_peers, announces, samples, fetch_failures)) STORED,
        parsed_metadata jsonb
    );
    '''),
    step('''
    CREATE TABLE announces (
        id integer NOT NULL PRIMARY KEY,
        ih bytea,
        node_id bytea,
        "time" timestamp without time zone,
        peer_ip inet,
        peer_port integer
    );
    '''),
    step('''
    CREATE TABLE nodes (
        id bytea NOT NULL PRIMARY KEY,
        ip inet NOT NULL,
        port integer NOT NULL,
        last_response_time timestamp without time zone,
        last_query_time timestamp without time zone
    );
    '''),
]
