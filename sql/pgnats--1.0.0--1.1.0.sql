ALTER TABLE pgnats.subscriptions RENAME TO subscriptions_old_v1_0_0;

CREATE TABLE pgnats.subscriptions (
    subject TEXT NOT NULL,
    fn_oid OID NOT NULL,
    UNIQUE(subject, fn_oid)
);

INSERT INTO pgnats.subscriptions (subject, fn_oid)
SELECT
    subject,
    to_regproc(callback) AS fn_oid
FROM
    pgnats.subscriptions_old_v1_0_0
WHERE
    to_regproc(callback) <> 0;

DROP TABLE pgnats.subscriptions_old_v1_0_0;

CREATE OR REPLACE FUNCTION pgnats.cleanup_subscriptions_on_drop()
RETURNS event_trigger AS $$
DECLARE
    obj record;
BEGIN
    FOR obj IN
        SELECT * FROM pg_event_trigger_dropped_objects()
    LOOP
        IF obj.object_type = 'function' THEN
            DELETE FROM pgnats.subscriptions
            WHERE fn_oid = obj.objid;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE EVENT TRIGGER pgnats_on_drop_function
ON sql_drop
WHEN TAG IN ('DROP FUNCTION')
EXECUTE FUNCTION pgnats.cleanup_subscriptions_on_drop();

DROP FUNCTION "pgnats_version"();

/* <begin connected objects> */
-- src/api/mod.rs:65
-- pgnats::api::pgnats_version
CREATE  FUNCTION "pgnats_version"() RETURNS TABLE (
	"version" TEXT,  /* alloc::string::String */
	"commit_date" TEXT,  /* alloc::string::String */
	"short_commit" TEXT,  /* alloc::string::String */
	"branch" TEXT,  /* alloc::string::String */
	"last_tag" TEXT  /* alloc::string::String */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pgnats_version_wrapper';
/* </end connected objects> */
