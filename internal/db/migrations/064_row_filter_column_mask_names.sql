-- +goose Up
ALTER TABLE row_filters ADD COLUMN name TEXT;
UPDATE row_filters
SET name = CASE
    WHEN description IS NOT NULL AND TRIM(description) <> '' THEN TRIM(description)
    ELSE 'filter_' || SUBSTR(id, 1, 8)
END
WHERE name IS NULL OR TRIM(name) = '';
CREATE UNIQUE INDEX idx_row_filters_table_name ON row_filters(table_id, name);

ALTER TABLE column_masks ADD COLUMN name TEXT;
UPDATE column_masks
SET name = CASE
    WHEN column_name IS NOT NULL AND TRIM(column_name) <> '' THEN TRIM(column_name)
    ELSE 'mask_' || SUBSTR(id, 1, 8)
END
WHERE name IS NULL OR TRIM(name) = '';
CREATE UNIQUE INDEX idx_column_masks_table_name ON column_masks(table_id, name);

-- +goose Down
DROP INDEX IF EXISTS idx_column_masks_table_name;
DROP INDEX IF EXISTS idx_row_filters_table_name;
