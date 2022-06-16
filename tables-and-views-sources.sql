-- public.calls_for_service_csv table DDL

CREATE TABLE public.calls_for_service_csv (
	x float4 NULL,
	y float4 NULL,
	objectid int4 NULL,
	primary_key varchar(32) NULL,
	final_case_type varchar(128) NULL,
	occ_year varchar(12) NULL,
	occ_dt varchar(128) NULL,
	obfaddress varchar(128) NULL,
	priority varchar(128) NULL,
	"zone" varchar(128) NULL,
	grid varchar(128) NULL,
	how_received varchar(128) NULL,
	unit_id1 varchar(128) NULL,
	unit_id2 varchar(128) NULL,
	unit_id3 varchar(128) NULL,
	report_flag varchar(128) NULL,
	case_status varchar(128) NULL,
	cleared_by varchar(128) NULL,
	x_rand varchar(128) NULL,
	y_rand varchar(128) NULL,
	disclaimer varchar(128) NULL,
	place_name varchar(128) NULL,
	cfstype varchar(128) NULL
);

-- public.general_offenses_csv table DDL
CREATE TABLE public.general_offenses_csv (
	x float4 NULL,
	y float4 NULL,
	objectid int4 NULL,
	primary_key varchar(32) NULL,
	occ_dt varchar(128) NULL,
	obfaddress varchar(128) NULL,
	x_rand int4 NULL,
	y_rand int4 NULL,
	disclaimer varchar(128) NULL,
	place_name varchar(128) NULL,
	offensecustom varchar(128) NULL,
	locationtranslation varchar(128) NULL,
	latitude float4 NULL,
	longitude float4 NULL,
	rucrcomp varchar(128) NULL
);


-- public.vw_offenses_calls_joined source

CREATE OR REPLACE VIEW public.vw_offenses_calls_joined
AS SELECT cfs.occ_year AS cfs_occ_year,
    cfs.occ_dt AS cfs_occ_dt,
    cfs.cfstype AS cfs_cfstype,
    gof.offensecustom AS gof_offensecustom,
    cfs.place_name AS cfs_place_name,
    gof.place_name AS gof_place_name,
    gof.locationtranslation AS gof_locationtranslation,
    cfs.obfaddress AS cfs_obfaddress,
    gof.obfaddress AS gof_obfaddress,
    cfs.how_received AS cfs_how_received,
    cfs.unit_id1 AS cfs_unit_id1,
    cfs.unit_id2 AS cfs_unit_id2,
    cfs.unit_id3 AS cfs_unit_id3,
    cfs.report_flag AS cfs_report_flag,
    cfs.case_status AS cfs_case_status,
    cfs.cleared_by AS cfs_cleared_by,
    cfs.disclaimer AS cfs_disclaimer,
    cfs.zone AS cfs_zone,
    cfs.grid AS cfs_grid,
    gof.disclaimer AS gof_disclaimer,
    gof.rucrcomp AS gof_rucrcomp,
    cfs.x AS cfs_x,
    cfs.y AS cfs_y,
    gof.x AS gof_x,
    gof.y AS gof_y,
    cfs.objectid AS cfs_objectid,
    gof.objectid AS gof_objectid,
    cfs.primary_key AS cfs_primary_key,
    gof.primary_key AS gof_primary_key,
    cfs.final_case_type AS cfs_final_case_type,
    cfs.priority AS cfs_priority,
    cfs.x_rand AS cfs_x_rand,
    cfs.y_rand AS cfs_y_rand,
    gof.x_rand AS gof_x_rand,
    gof.y_rand AS gof_y_rand,
    gof.latitude AS gof_latitude,
    gof.longitude AS gof_longitude,
    gof.occ_dt AS gof_occ_dt
   FROM calls_for_service_csv cfs
     LEFT JOIN general_offenses_csv gof ON cfs.primary_key::text = gof.primary_key::text
  ORDER BY gof.longitude, gof.latitude, gof.place_name, gof.occ_dt DESC;

-- public.vw_offenses_calls_counts source

CREATE OR REPLACE VIEW public.vw_offenses_calls_counts
AS SELECT vw_offenses_calls_joined.cfs_occ_year AS occ_year,
    COALESCE(vw_offenses_calls_joined.gof_offensecustom, vw_offenses_calls_joined.cfs_cfstype) AS gof_cfs_offense,
    COALESCE(vw_offenses_calls_joined.gof_place_name, vw_offenses_calls_joined.cfs_place_name) AS gof_cfs_place_name,
    COALESCE(vw_offenses_calls_joined.gof_obfaddress, vw_offenses_calls_joined.cfs_obfaddress) AS obfaddress,
    count(*) AS call_count
   FROM vw_offenses_calls_joined
  GROUP BY vw_offenses_calls_joined.cfs_occ_year, vw_offenses_calls_joined.gof_offensecustom, vw_offenses_calls_joined.cfs_cfstype, vw_offenses_calls_joined.cfs_place_name, vw_offenses_calls_joined.gof_place_name, vw_offenses_calls_joined.gof_obfaddress, vw_offenses_calls_joined.cfs_obfaddress
  ORDER BY vw_offenses_calls_joined.cfs_occ_year DESC, (count(*)) DESC;

-- public.vw_cfs_cfstypes_counts source

CREATE OR REPLACE VIEW public.vw_cfs_cfstypes_counts
AS SELECT count(*) AS call_count,
    cfs.place_name,
    cfs.cfstype
   FROM calls_for_service_csv cfs
  GROUP BY cfs.place_name, cfs.cfstype
  ORDER BY cfs.cfstype;

