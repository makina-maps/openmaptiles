DROP TRIGGER IF EXISTS trigger_osm_transportation_merge_linestring ON osm_transportation_merge_linestring;
DROP TRIGGER IF EXISTS trigger_store_transportation_highway_linestring ON osm_highway_linestring;
DROP TRIGGER IF EXISTS trigger_flag_transportation ON osm_highway_linestring;
DROP TRIGGER IF EXISTS trigger_refresh ON transportation.updates;

-- Instead of using relations to find out the road names we
-- stitch together the touching ways with the same name
-- to allow for nice label rendering
-- Because this works well for roads that do not have relations as well


-- Improve performance of the sql in transportation_name/update_route_member.sql
CREATE INDEX IF NOT EXISTS osm_highway_linestring_highway_partial_idx
    ON osm_highway_linestring (highway)
    WHERE highway IN ('motorway', 'trunk');

-- Index to speed up diff update
CREATE INDEX IF NOT EXISTS osm_highway_linestring_geom_partial_idx
    ON osm_highway_linestring
    USING gist(geometry)
    WHERE (highway IN ('motorway', 'trunk', 'primary') OR
          highway = 'construction' AND construction IN ('motorway', 'trunk', 'primary'))
      AND ST_IsValid(geometry);


CREATE TABLE IF NOT EXISTS osm_transportation_merge_linestring (
    geometry geometry,
    id SERIAL PRIMARY KEY,
    highway character varying,
    construction character varying,
    is_bridge boolean,
    is_tunnel boolean,
    is_ford boolean,
    z_order integer
);

-- etldoc: osm_highway_linestring ->  osm_transportation_merge_linestring
INSERT INTO osm_transportation_merge_linestring (geometry, highway, construction, is_bridge, is_tunnel, is_ford, z_order)
SELECT (ST_Dump(geometry)).geom AS geometry,
       highway,
       construction,
       is_bridge,
       is_tunnel,
       is_ford,
       z_order
FROM (
         SELECT ST_LineMerge(ST_Collect(geometry)) AS geometry,
                highway,
                construction,
                is_bridge,
                is_tunnel,
                is_ford,
                min(z_order) AS z_order
         FROM osm_highway_linestring
         WHERE (highway IN ('motorway', 'trunk', 'primary') OR
                highway = 'construction' AND construction IN ('motorway', 'trunk', 'primary'))
           AND ST_IsValid(geometry)
         GROUP BY highway, construction, is_bridge, is_tunnel, is_ford
     ) AS highway_union;
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_geometry_idx
    ON osm_transportation_merge_linestring USING gist (geometry);

CREATE TABLE IF NOT EXISTS osm_transportation_merge_linestring_gen_z8
    (LIKE osm_transportation_merge_linestring);

CREATE TABLE IF NOT EXISTS osm_transportation_merge_linestring_gen_z7
    (LIKE osm_transportation_merge_linestring_gen_z8);

CREATE TABLE IF NOT EXISTS osm_transportation_merge_linestring_gen_z6
    (LIKE osm_transportation_merge_linestring_gen_z7);

CREATE TABLE IF NOT EXISTS osm_transportation_merge_linestring_gen_z5
    (LIKE osm_transportation_merge_linestring_gen_z6);

CREATE TABLE IF NOT EXISTS osm_transportation_merge_linestring_gen_z4
    (LIKE osm_transportation_merge_linestring_gen_z5);

CREATE OR REPLACE FUNCTION insert_transportation_merge_linestring_gen(update_id bigint) RETURNS void AS
$$
BEGIN
    -- etldoc: osm_transportation_merge_linestring -> osm_transportation_merge_linestring_gen_z8
    INSERT INTO osm_transportation_merge_linestring_gen_z8
    SELECT ST_Simplify(geometry, ZRes(10)) AS geometry,
        id,
        highway,
        construction,
        is_bridge,
        is_tunnel,
        is_ford,
        z_order
    FROM osm_transportation_merge_linestring
    WHERE
        (update_id IS NULL OR id = update_id) AND
        (highway IN ('motorway', 'trunk', 'primary') OR
            highway = 'construction' AND construction IN ('motorway', 'trunk', 'primary'));

    -- etldoc: osm_transportation_merge_linestring_gen_z8 -> osm_transportation_merge_linestring_gen_z7
    INSERT INTO osm_transportation_merge_linestring_gen_z7
    SELECT ST_Simplify(geometry, ZRes(9)) AS geometry,
        id,
        highway,
        construction,
        is_bridge,
        is_tunnel,
        is_ford,
        z_order
    FROM osm_transportation_merge_linestring_gen_z8
    WHERE
        (update_id IS NULL OR id = update_id) AND
        (highway IN ('motorway', 'trunk', 'primary') OR highway = 'construction' AND construction IN ('motorway', 'trunk', 'primary')) AND
        ST_Length(geometry) > 50;

    -- etldoc: osm_transportation_merge_linestring_gen_z7 -> osm_transportation_merge_linestring_gen_z6
    INSERT INTO osm_transportation_merge_linestring_gen_z6
    SELECT ST_Simplify(geometry, ZRes(8)) AS geometry,
        id,
        highway,
        construction,
        is_bridge,
        is_tunnel,
        is_ford,
        z_order
    FROM osm_transportation_merge_linestring_gen_z7
    WHERE
        (update_id IS NULL OR id = update_id) AND
        (highway IN ('motorway', 'trunk') OR highway = 'construction' AND construction IN ('motorway', 'trunk')) AND
        ST_Length(geometry) > 100;

    -- etldoc: osm_transportation_merge_linestring_gen_z6 -> osm_transportation_merge_linestring_gen_z5
    INSERT INTO osm_transportation_merge_linestring_gen_z5
    SELECT ST_Simplify(geometry, ZRes(7)) AS geometry,
        id,
        highway,
        construction,
        is_bridge,
        is_tunnel,
        is_ford,
        z_order
    FROM osm_transportation_merge_linestring_gen_z6
    WHERE
        (update_id IS NULL OR id = update_id) AND
        (highway IN ('motorway', 'trunk') OR highway = 'construction' AND construction IN ('motorway', 'trunk')) AND
        ST_Length(geometry) > 500;

    -- etldoc: osm_transportation_merge_linestring_gen_z5 -> osm_transportation_merge_linestring_gen_z4
    INSERT INTO osm_transportation_merge_linestring_gen_z4
    SELECT ST_Simplify(geometry, ZRes(6)) AS geometry,
        id,
        highway,
        construction,
        is_bridge,
        is_tunnel,
        is_ford,
        z_order
    FROM osm_transportation_merge_linestring_gen_z5
    WHERE
        (update_id IS NULL OR id = update_id) AND
        (highway = 'motorway' OR highway = 'construction' AND construction = 'motorway') AND
        ST_Length(geometry) > 1000;
END;
$$ LANGUAGE plpgsql;


SELECT insert_transportation_merge_linestring_gen(NULL);

CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z8_geometry_idx
    ON osm_transportation_merge_linestring_gen_z8 USING gist (geometry);
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z8_id_idx
    ON osm_transportation_merge_linestring_gen_z8(id);

CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z7_geometry_idx
    ON osm_transportation_merge_linestring_gen_z7 USING gist (geometry);
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z7_id_idx
    ON osm_transportation_merge_linestring_gen_z7(id);

CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z6_geometry_idx
    ON osm_transportation_merge_linestring_gen_z6 USING gist (geometry);
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z6_id_idx
    ON osm_transportation_merge_linestring_gen_z6(id);

CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z5_geometry_idx
    ON osm_transportation_merge_linestring_gen_z5 USING gist (geometry);
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z5_id_idx
    ON osm_transportation_merge_linestring_gen_z5(id);

CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z4_geometry_idx
    ON osm_transportation_merge_linestring_gen_z4 USING gist (geometry);
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z4_id_idx
    ON osm_transportation_merge_linestring_gen_z4(id);


-- Handle updates

CREATE SCHEMA IF NOT EXISTS transportation;

CREATE TABLE IF NOT EXISTS transportation.changes
(
    id serial PRIMARY KEY,
    osm_id bigint,
    is_old boolean,
    geometry geometry,
    highway varchar,
    construction varchar,
    is_bridge boolean,
    is_tunnel boolean,
    is_ford boolean,
    z_order integer
);

CREATE OR REPLACE FUNCTION transportation.store() RETURNS trigger AS
$$
BEGIN
    IF (tg_op = 'DELETE' OR tg_op = 'UPDATE') THEN
        INSERT INTO transportation.changes(osm_id, is_old, geometry, highway, construction, is_bridge, is_tunnel, is_ford, z_order)
        VALUES (old.osm_id, true, old.geometry, old.highway, old.construction, old.is_bridge, old.is_tunnel, old.is_ford, old.z_order);
    END IF;
    IF (tg_op = 'UPDATE' OR tg_op = 'INSERT') THEN
        INSERT INTO transportation.changes(osm_id, is_old, geometry, highway, construction, is_bridge, is_tunnel, is_ford, z_order)
        VALUES (new.osm_id, false, new.geometry, new.highway, new.construction, new.is_bridge, new.is_tunnel, new.is_ford, new.z_order);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS transportation.updates
(
    id serial PRIMARY KEY,
    t text,
    UNIQUE (t)
);
CREATE OR REPLACE FUNCTION transportation.flag() RETURNS trigger AS
$$
BEGIN
    INSERT INTO transportation.updates(t) VALUES ('y') ON CONFLICT(t) DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION transportation.refresh() RETURNS trigger AS
$$
DECLARE
    t TIMESTAMP WITH TIME ZONE := clock_timestamp();
BEGIN
    RAISE LOG 'Refresh transportation';

    -- Compact the change history to keep only the first and last version
    CREATE TEMP TABLE changes_compact AS
    SELECT
        osm_id,
        is_old,
        geometry,
        highway,
        construction,
        is_bridge,
        is_tunnel,
        is_ford,
        z_order
    FROM ((
              SELECT DISTINCT ON (osm_id) *
              FROM transportation.changes
              WHERE is_old
              ORDER BY osm_id,
                       id ASC
          )
          UNION ALL
          (
              SELECT DISTINCT ON (osm_id) *
              FROM transportation.changes
              WHERE NOT is_old
              ORDER BY osm_id,
                       id DESC
          )) AS t;

    -- Collect all original existing ways from impacted mmerge
    CREATE TEMP TABLE osm_highway_linestring_original AS
    SELECT DISTINCT ON (geometry, highway, construction, is_bridge, is_tunnel, is_ford)
        h.geometry,
        h.highway,
        h.construction,
        h.is_bridge,
        h.is_tunnel,
        h.is_ford,
        h.z_order
    FROM
        osm_transportation_merge_linestring AS t
        JOIN changes_compact AS c ON
            t.geometry && c.geometry
            AND t.highway IS NOT DISTINCT FROM c.highway
            AND t.construction IS NOT DISTINCT FROM c.construction
            AND t.is_bridge IS NOT DISTINCT FROM c.is_bridge
            AND t.is_tunnel IS NOT DISTINCT FROM c.is_tunnel
            AND t.is_ford IS NOT DISTINCT FROM c.is_ford
        JOIN osm_highway_linestring AS h ON
            NOT h.osm_id IN (SELECT osm_id FROM changes_compact)
            AND t.geometry && c.geometry
            AND ST_Contains(t.geometry, h.geometry)
            AND t.highway IS NOT DISTINCT FROM c.highway
            AND t.construction IS NOT DISTINCT FROM c.construction
            AND t.is_bridge IS NOT DISTINCT FROM c.is_bridge
            AND t.is_tunnel IS NOT DISTINCT FROM c.is_tunnel
            AND t.is_ford IS NOT DISTINCT FROM c.is_ford
    WHERE (h.highway IN ('motorway', 'trunk', 'primary') OR
            h.highway = 'construction' AND h.construction IN ('motorway', 'trunk', 'primary'))
        AND ST_IsValid(h.geometry);

    DELETE
    FROM osm_transportation_merge_linestring AS t
        USING changes_compact AS c
    WHERE
        t.geometry && c.geometry
        AND t.highway IS NOT DISTINCT FROM c.highway
        AND t.construction IS NOT DISTINCT FROM c.construction
        AND t.is_bridge IS NOT DISTINCT FROM c.is_bridge
        AND t.is_tunnel IS NOT DISTINCT FROM c.is_tunnel
        AND t.is_ford IS NOT DISTINCT FROM c.is_ford;

    INSERT INTO osm_transportation_merge_linestring (geometry, highway, construction, is_bridge, is_tunnel, is_ford, z_order)
    SELECT (ST_Dump(geometry)).geom AS geometry,
        highway,
        construction,
        is_bridge,
        is_tunnel,
        is_ford,
        z_order
    FROM (
        SELECT ST_LineMerge(ST_Collect(geometry)) AS geometry,
                highway,
                construction,
                is_bridge,
                is_tunnel,
                is_ford,
                min(z_order) AS z_order
        FROM ((
            SELECT * FROM osm_highway_linestring_original
        ) UNION ALL (
            -- New or updated ways
            SELECT
                geometry,
                highway,
                construction,
                is_bridge,
                is_tunnel,
                is_ford,
                z_order
            FROM
                changes_compact
            WHERE
                NOT is_old
                AND (highway IN ('motorway', 'trunk', 'primary') OR
                      highway = 'construction' AND construction IN ('motorway', 'trunk', 'primary'))
                AND ST_IsValid(geometry)
        )) AS t
        GROUP BY highway, construction, is_bridge, is_tunnel, is_ford
    ) AS highway_union;

    DROP TABLE osm_highway_linestring_original;
    DROP TABLE changes_compact;
    -- noinspection SqlWithoutWhere
    DELETE FROM transportation.changes;
    -- noinspection SqlWithoutWhere
    DELETE FROM transportation.updates;

    RAISE LOG 'Refresh transportation done in %', age(clock_timestamp(), t);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION transportation.merge_linestring_gen_refresh() RETURNS trigger AS
$$
BEGIN
    IF (tg_op = 'DELETE') THEN
        DELETE FROM osm_transportation_merge_linestring_gen_z8 WHERE id = old.id;
        DELETE FROM osm_transportation_merge_linestring_gen_z7 WHERE id = old.id;
        DELETE FROM osm_transportation_merge_linestring_gen_z6 WHERE id = old.id;
        DELETE FROM osm_transportation_merge_linestring_gen_z5 WHERE id = old.id;
        DELETE FROM osm_transportation_merge_linestring_gen_z4 WHERE id = old.id;
    END IF;

    IF (tg_op = 'UPDATE' OR tg_op = 'INSERT') THEN
        PERFORM insert_transportation_merge_linestring_gen(new.id);
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_osm_transportation_merge_linestring
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_transportation_merge_linestring
    FOR EACH ROW
EXECUTE PROCEDURE transportation.merge_linestring_gen_refresh();

CREATE TRIGGER trigger_store_transportation_highway_linestring
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_highway_linestring
    FOR EACH ROW
EXECUTE PROCEDURE transportation.store();

CREATE TRIGGER trigger_flag_transportation
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_highway_linestring
    FOR EACH STATEMENT
EXECUTE PROCEDURE transportation.flag();

CREATE CONSTRAINT TRIGGER trigger_refresh
    AFTER INSERT
    ON transportation.updates
    INITIALLY DEFERRED
    FOR EACH ROW
EXECUTE PROCEDURE transportation.refresh();