-- +goose Up
ALTER TABLE dashboard_widgets ADD COLUMN filter_origin_key TEXT NOT NULL DEFAULT '';

WITH widget_keys AS (
    SELECT
        id,
        dashboard_id,
        CASE
            WHEN TRIM(COALESCE(json_extract(visual_spec, '$.kind'), '')) = '' THEN 'widget'
            ELSE LOWER(TRIM(COALESCE(json_extract(visual_spec, '$.kind'), 'widget')))
        END AS kind_slug,
        TRIM(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(LOWER(name), ' ', '-'),
                                '_', '-'),
                            '/', '-'),
                        '.', '-'),
                    ',', ''),
                '(', ''),
            ')', '')
        ) AS name_slug,
        created_at
    FROM dashboard_widgets
),
ranked AS (
    SELECT
        id,
        dashboard_id,
        CASE
            WHEN name_slug = '' THEN kind_slug
            ELSE kind_slug || '-' || name_slug
        END AS base_key,
        ROW_NUMBER() OVER (
            PARTITION BY dashboard_id,
            CASE
                WHEN name_slug = '' THEN kind_slug
                ELSE kind_slug || '-' || name_slug
            END
            ORDER BY created_at, id
        ) AS ordinal
    FROM widget_keys
),
final_keys AS (
    SELECT
        id,
        CASE
            WHEN ordinal = 1 THEN base_key
            ELSE base_key || '-' || ordinal
        END AS filter_origin_key
    FROM ranked
)
UPDATE dashboard_widgets
SET filter_origin_key = (
    SELECT filter_origin_key
    FROM final_keys
    WHERE final_keys.id = dashboard_widgets.id
);

CREATE UNIQUE INDEX idx_dashboard_widgets_dashboard_filter_origin_key
    ON dashboard_widgets(dashboard_id, filter_origin_key);

-- +goose Down
DROP INDEX IF EXISTS idx_dashboard_widgets_dashboard_filter_origin_key;
